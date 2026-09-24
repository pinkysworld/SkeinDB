//! Row-level redo write-ahead log for crash recovery of table data.
//!
//! Each committed DML mutation (insert/update/delete) appends the full final image of
//! every changed row to its database's own WAL (`<data_dir>/wal/<db>/wal-000001.log`) and
//! fsyncs it *before* the table snapshot is written. On the next [`Engine::open`] every
//! database's committed records are replayed onto the loaded snapshots. Redo is idempotent:
//! each record carries the row's full final image plus its monotonic `version` and
//! `commit_ts_ms`, so re-applying a record the snapshot already holds is a no-op (the
//! version check skips it), while a mutation lost between its WAL commit and the snapshot
//! write is recovered. After replay — and at every checkpoint — the recovered tables are
//! re-persisted and the WALs are truncated to a clean state.
//!
//! Partitioning the WAL per database (rather than one shared log) keeps one database's
//! append + group-commit fsync independent of another's; that independence is the
//! durability-side prerequisite for per-database write-lock sharding (see
//! `docs/PERFORMANCE.md` §4b). A single DML statement only ever touches one database, so a
//! WAL transaction never spans databases and the partition introduces no cross-database
//! atomicity change. A legacy single global WAL (`<data_dir>/wal-000001.log`) written by an
//! earlier version is drained and removed on the first open after upgrade.
//!
//! This closes the "crash safety between persists is best-effort" gap: the table
//! snapshot and the WAL are written in a fixed order (WAL fsync, then snapshot), so a
//! crash at any point leaves a state the next open can reconstruct. See
//! `docs/ON_DISK_FORMAT.md` for the on-disk record layout.

use super::*;
use skeindb_core::wal::{WalReader, WalWriter};

/// Number of row mutations to absorb into the WAL before flushing dirty table snapshots
/// and truncating the log. Bounds both the on-disk WAL size and crash-recovery replay
/// time while keeping the per-mutation hot path free of full-table snapshot rewrites.
pub(crate) const WAL_FLUSH_THRESHOLD: u64 = 128;

/// One row redo record: the full committed state of a single row after a mutation.
/// `deleted` rows are logged too (a delete is a redo that tombstones the row).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WalRowRecord {
    pub db: String,
    pub table: String,
    #[serde(default)]
    pub incarnation_id: u64,
    pub pk: Vec<Lit>,
    pub row: RowObject,
    pub version: u64,
    pub schema_version: u64,
    pub deleted: bool,
    pub commit_ts_ms: u64,
    #[serde(default)]
    pub auto_inc_next: Option<HashMap<String, u64>>,
}

impl WalRowRecord {
    /// Build a redo record from a row entry that was just written in memory.
    pub(crate) fn from_entry(
        db: &str,
        table: &str,
        incarnation_id: u64,
        pk: Vec<Lit>,
        entry: &RowEntry,
    ) -> Self {
        Self {
            db: db.to_string(),
            table: table.to_string(),
            incarnation_id,
            pk,
            row: entry.row.clone(),
            version: entry.version,
            schema_version: entry.schema_version,
            deleted: entry.deleted,
            commit_ts_ms: entry.commit_ts_ms,
            auto_inc_next: None,
        }
    }
}

const WAL_ROW_RECORD_FORMAT_VERSION: u32 = 3;
const WAL_ROW_RECORD_COLUMN: &str = "$skein_wal_record";

pub(super) fn wal_row_record_column(incarnation_id: u64) -> String {
    format!("{WAL_ROW_RECORD_COLUMN}:{incarnation_id}")
}

/// Version 2 wraps all row and primary-key data in one payload. On encrypted databases the
/// payload is encrypted as a single cell, so neither variable-length column values nor string
/// primary keys are exposed by the redo log. Version-1 records used the `WalRowRecord` shape
/// directly and remain readable for upgrades.
#[derive(Debug, Serialize, Deserialize)]
struct WalRowRecordDisk {
    format_version: u32,
    db: String,
    table: String,
    #[serde(default)]
    incarnation_id: u64,
    payload: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct WalRowPayload {
    pk: Vec<Lit>,
    row: RowObject,
    version: u64,
    schema_version: u64,
    deleted: bool,
    commit_ts_ms: u64,
    #[serde(default)]
    auto_inc_next: Option<HashMap<String, u64>>,
}

impl Engine {
    fn wal_encode_record(&self, record: &WalRowRecord) -> anyhow::Result<Vec<u8>> {
        let payload = serde_json::to_value(WalRowPayload {
            pk: record.pk.clone(),
            row: record.row.clone(),
            version: record.version,
            schema_version: record.schema_version,
            deleted: record.deleted,
            commit_ts_ms: record.commit_ts_ms,
            auto_inc_next: record.auto_inc_next.clone(),
        })?;
        let codec = self.row_encryption_codec(&record.db, &record.table);
        if codec.requires_key_but_unavailable() {
            anyhow::bail!(
                "encryption key required: database '{}' has encryption enabled but its active key is unavailable",
                record.db
            );
        }
        let payload = if codec.active() {
            codec
                .encrypt_cell(
                    &wal_row_record_column(record.incarnation_id),
                    &Lit::Json { v: payload.clone() },
                )?
                .ok_or_else(|| anyhow::anyhow!("WAL payload could not be encrypted"))?
        } else {
            payload
        };
        serde_json::to_vec(&WalRowRecordDisk {
            format_version: WAL_ROW_RECORD_FORMAT_VERSION,
            db: record.db.clone(),
            table: record.table.clone(),
            incarnation_id: record.incarnation_id,
            payload,
        })
        .map_err(Into::into)
    }

    fn wal_decode_record(&self, value: serde_json::Value) -> anyhow::Result<WalRowRecord> {
        // Before format versioning, records were serialized directly as WalRowRecord.
        if value.get("format_version").is_none() {
            return serde_json::from_value(value).map_err(Into::into);
        }
        let format_version = value
            .get("format_version")
            .and_then(serde_json::Value::as_u64)
            .and_then(|version| u32::try_from(version).ok())
            .ok_or_else(|| anyhow::anyhow!("WAL row record format version is malformed"))?;
        if format_version != 2 && format_version != WAL_ROW_RECORD_FORMAT_VERSION {
            anyhow::bail!("unsupported WAL row record version: {}", format_version);
        }
        if format_version == WAL_ROW_RECORD_FORMAT_VERSION
            && value
                .get("incarnation_id")
                .and_then(serde_json::Value::as_u64)
                .is_none()
        {
            anyhow::bail!("WAL row record incarnation id is malformed");
        }
        let disk: WalRowRecordDisk = serde_json::from_value(value)?;
        let payload = if let Some(encrypted) = decode_encrypted_cell_payload(&disk.payload) {
            let expected_column = if format_version == 2 {
                WAL_ROW_RECORD_COLUMN.to_string()
            } else {
                wal_row_record_column(disk.incarnation_id)
            };
            if encrypted.column != expected_column {
                anyhow::bail!("WAL row record has an unexpected encrypted payload column");
            }
            let codec = self.row_encryption_codec(&disk.db, &disk.table);
            codec
                .decrypt_cell(&encrypted.column, &encrypted.kind, &encrypted.env_b64)
                .map_err(|err| {
                    anyhow::Error::from(EncryptionLockedError {
                        db: disk.db.clone(),
                        table: disk.table.clone(),
                        detail: err.to_string(),
                    })
                })?
        } else {
            Lit::Json { v: disk.payload }
        };
        let Lit::Json { v: payload } = payload else {
            anyhow::bail!("WAL row record payload is not a JSON object");
        };
        let payload: WalRowPayload = serde_json::from_value(payload)?;
        Ok(WalRowRecord {
            db: disk.db,
            table: disk.table,
            incarnation_id: disk.incarnation_id,
            pk: payload.pk,
            row: payload.row,
            version: payload.version,
            schema_version: payload.schema_version,
            deleted: payload.deleted,
            commit_ts_ms: payload.commit_ts_ms,
            auto_inc_next: payload.auto_inc_next,
        })
    }
}

/// One database's write-ahead log: its append-only writer plus the count of committed
/// transactions appended but not yet fsynced (bounded by `Engine::wal_sync_batch`; reset at
/// each fsync and when the database's WAL is truncated). Held in `Engine::wals`, keyed by
/// database name, and opened lazily on that database's first mutation.
#[derive(Debug)]
pub(crate) struct DbWal {
    pub(crate) writer: WalWriter,
    pub(crate) unsynced_commits: u64,
}

impl Engine {
    /// Root directory holding every database's WAL sidecar directory.
    pub(crate) fn wal_dir(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    /// Path to one database's WAL segment (`<data_dir>/wal/<db>/wal-000001.log`).
    pub(crate) fn wal_db_path(&self, db: &str) -> PathBuf {
        self.wal_dir().join(db).join("wal-000001.log")
    }

    /// Path to the pre-partitioning single global WAL. Present only until the first open
    /// after upgrading from a version that wrote one shared log; drained then removed.
    pub(crate) fn legacy_wal_path(&self) -> PathBuf {
        self.data_dir.join("wal-000001.log")
    }

    /// Database names that currently have a WAL segment on disk under `wal/`.
    fn wal_dbs_on_disk(&self) -> Vec<String> {
        let mut dbs = Vec::new();
        if let Ok(entries) = fs::read_dir(self.wal_dir()) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Some(name) = entry.file_name().to_str() {
                        if self.wal_db_path(name).exists() {
                            dbs.push(name.to_string());
                        }
                    }
                }
            }
        }
        dbs
    }

    /// Append a batch of row redo records as committed WAL transactions (one per database).
    /// Called by the DML handlers *before* they persist the table snapshot, so a crash after
    /// this returns can always recover the mutation on the next open().
    ///
    /// The record is always appended (durable to the OS page cache, so it survives a process
    /// crash and is recovered on a clean restart). The `fsync` — which is what forces it to
    /// stable storage and survives a power-loss crash — is issued every `wal_sync_batch`
    /// commits (group commit) per database. With the default batch of 1 that is every commit
    /// (unchanged, strongest durability); a larger batch amortizes the fsync-under-lock cost.
    /// Recovery is a consistent committed prefix regardless of where an fsync fell (see
    /// `wal_recovery_is_robust_to_torn_tail_at_every_offset`), and the deferred snapshot flush
    /// makes everything durable at each checkpoint.
    pub(crate) fn wal_log_rows(&mut self, records: &[WalRowRecord]) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        // Group by database so each database's redo lands in its own WAL. Every current
        // caller (`data_insert`/`data_update`/`data_delete`) passes a single database's rows,
        // so this is normally one group; grouping keeps the invariant explicit and correct
        // even if a future caller batches across databases.
        let mut by_db: BTreeMap<&str, Vec<&WalRowRecord>> = BTreeMap::new();
        for rec in records {
            by_db.entry(rec.db.as_str()).or_default().push(rec);
        }
        for (db, recs) in by_db {
            self.wal_log_rows_for_db(db, &recs)?;
        }
        Ok(())
    }

    /// Append one database's redo records as a single committed transaction to that
    /// database's WAL, opening the writer lazily and fsyncing per the group-commit batch.
    /// See [`Engine::wal_log_rows`] for the durability contract.
    fn wal_log_rows_for_db(&mut self, db: &str, records: &[&WalRowRecord]) -> anyhow::Result<()> {
        let txn = self.wal_next_txn;
        self.wal_next_txn = txn
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("WAL transaction id space exhausted"))?;
        let encoded_records = records
            .iter()
            .map(|record| self.wal_encode_record(record))
            .collect::<anyhow::Result<Vec<_>>>()?;
        if !self.wals.contains_key(db) {
            let dir = self.wal_dir().join(db);
            fs::create_dir_all(&dir)?;
            let writer = WalWriter::open(self.wal_db_path(db))?;
            self.wals.insert(
                db.to_string(),
                DbWal {
                    writer,
                    unsynced_commits: 0,
                },
            );
        }
        let batch = self.wal_sync_batch;
        let dbwal = self.wals.get_mut(db).expect("wal writer opened above");
        dbwal.writer.begin_txn(txn)?;
        for encoded in encoded_records {
            dbwal.writer.append_mutation(txn, encoded)?;
        }
        dbwal.writer.commit_txn(txn)?;
        dbwal.unsynced_commits = dbwal.unsynced_commits.saturating_add(1);
        if dbwal.unsynced_commits >= batch {
            #[cfg(test)]
            let sync_result = if std::mem::take(&mut self.wal_sync_fail_next_for_test) {
                Err(anyhow::anyhow!("injected WAL fsync failure"))
            } else {
                dbwal.writer.sync().map_err(anyhow::Error::from)
            };
            #[cfg(not(test))]
            let sync_result = dbwal.writer.sync().map_err(anyhow::Error::from);
            if let Err(err) = sync_result {
                self.wal_write_blocked_reason = Some(
                    "a WAL commit frame was written but fsync failed; restart to resolve the commit outcome"
                        .to_string(),
                );
                self.wal_recovery_incomplete = true;
                return Err(err.context("WAL commit is present but its durability is uncertain"));
            }
            dbwal.unsynced_commits = 0;
        }
        Ok(())
    }

    /// Replay committed WAL records after snapshots are loaded on open(). Idempotent;
    /// recovers any mutation that did not reach its table snapshot before a crash, then
    /// re-persists the affected tables and truncates the WAL to a clean checkpoint.
    pub(crate) fn wal_recover(&mut self) {
        // Sources: a legacy single global WAL (present only right after upgrading from a
        // version that wrote one shared log), then every per-database WAL. Replay is
        // idempotent, so the order between them does not matter.
        let mut sources: Vec<PathBuf> = Vec::new();
        let legacy = self.legacy_wal_path();
        if legacy.exists() {
            sources.push(legacy);
        }
        for db in self.wal_dbs_on_disk() {
            sources.push(self.wal_db_path(&db));
        }
        if sources.is_empty() {
            self.wal_recovery_incomplete = false;
            return;
        }

        let mut touched: BTreeSet<(String, String)> = BTreeSet::new();
        let mut recovery_incomplete = false;
        let mut current_records_seen = false;
        for path in &sources {
            let Ok(reader) = WalReader::open(path) else {
                recovery_incomplete = true;
                continue;
            };
            let Ok(recovery) = reader.recover() else {
                recovery_incomplete = true;
                continue;
            };
            if let Some(max_txn_id) = recovery.max_txn_id {
                self.wal_next_txn = self.wal_next_txn.max(max_txn_id.saturating_add(1));
            }
            for txn in &recovery.txns {
                for mutation in &txn.mutations {
                    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&mutation.payload)
                    else {
                        recovery_incomplete = true;
                        continue;
                    };
                    // A committed row for a dropped or recreated table is obsolete and can
                    // be discarded without decoding its (possibly encrypted) row payload.
                    let Some((db, table)) = (match self.wal_record_current_table(&value) {
                        Ok(target) => target,
                        Err(()) => {
                            recovery_incomplete = true;
                            continue;
                        }
                    }) else {
                        continue;
                    };
                    current_records_seen = true;
                    let key = TableKey {
                        db: db.to_string(),
                        table: table.to_string(),
                    };
                    // Encryption keys are intentionally not persisted. Keep the WAL and
                    // protect even an empty snapshot from writes until the key is registered.
                    if self.encrypted_locked_tables.contains(&key) {
                        recovery_incomplete = true;
                        continue;
                    }
                    let rec = match self.wal_decode_record(value) {
                        Ok(rec) => rec,
                        Err(err) => {
                            if is_encryption_locked_error(&err) {
                                self.encrypted_locked_tables.insert(key);
                            }
                            recovery_incomplete = true;
                            continue;
                        }
                    };
                    match self.wal_apply_record(&rec) {
                        Ok(true) => {
                            touched.insert((rec.db, rec.table));
                        }
                        Ok(false) => {}
                        Err(_) => recovery_incomplete = true,
                    }
                }
            }
        }
        if !touched.is_empty() {
            // Re-intern recovered cell values into the content-addressed ValueStore: the
            // store was built from the (stale) snapshots before replay, so without this
            // dedup/value-ref stats and value-ref planning would omit recovered rows.
            self.rebuild_value_store_from_tables_best_effort();
        }
        let mut all_persisted = true;
        for (db, table) in &touched {
            // Rebuild pk/secondary indexes from the recovered rows, then re-persist.
            let _ = self.rebuild_indexes_after_history_gc(db, table);
            // A persist can be refused (e.g. the table's on-disk file is corrupt or
            // encryption-locked). Don't truncate the WAL in that case, or the recovered
            // rows would be lost on the next open.
            if self.persist_table(db, table).is_err() {
                all_persisted = false;
            }
        }
        if !recovery_incomplete && current_records_seen && self.persist_catalog().is_err() {
            all_persisted = false;
        }
        self.wal_recovery_incomplete = !all_persisted || recovery_incomplete;
        if !self.wal_recovery_incomplete {
            self.wal_truncate();
        }
    }

    /// `Ok(None)` means a well-formed record belongs to a dropped or different table
    /// incarnation. `Err(())` is malformed or from an unsupported format and must keep the
    /// whole WAL intact so recovery never mistakes damaged metadata for an obsolete record.
    fn wal_record_current_table(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<Option<(String, String)>, ()> {
        let Some(object) = value.as_object() else {
            return Err(());
        };
        let db = object
            .get("db")
            .and_then(serde_json::Value::as_str)
            .ok_or(())?;
        let table = object
            .get("table")
            .and_then(serde_json::Value::as_str)
            .ok_or(())?;
        if validate_storage_name(db, "database").is_err()
            || validate_storage_name(table, "table").is_err()
        {
            return Err(());
        }
        let format_version = object
            .get("format_version")
            .and_then(serde_json::Value::as_u64);
        let incarnation_id = match object.get("format_version") {
            None => match object.get("incarnation_id") {
                None => 0,
                Some(value) => value.as_u64().ok_or(())?,
            },
            Some(version) => {
                let version = version.as_u64().ok_or(())?;
                if version != 2 && version != WAL_ROW_RECORD_FORMAT_VERSION as u64 {
                    return Err(());
                }
                object
                    .get("incarnation_id")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(())?
            }
        };
        let Ok(schema) = self.get_schema(db, table) else {
            return Ok(None);
        };
        if format_version == Some(2)
            && object
                .get("payload")
                .and_then(decode_encrypted_cell_payload)
                .is_some()
            && schema.incarnation_id != 0
        {
            let key = TableKey {
                db: db.to_string(),
                table: table.to_string(),
            };
            self.wal_write_blocked_tables.insert(
                key,
                "retained encrypted WAL format 2 does not authenticate its table incarnation; resolve or remove the WAL before writing this table".to_string(),
            );
            return Err(());
        }
        if schema.incarnation_id != incarnation_id {
            return Ok(None);
        }
        Ok(Some((db.to_string(), table.to_string())))
    }

    /// Apply a single redo record idempotently. Returns true if the in-memory table was
    /// changed (i.e. the record was newer than what the loaded snapshot already held).
    fn wal_apply_record(&mut self, rec: &WalRowRecord) -> anyhow::Result<bool> {
        if validate_storage_name(&rec.db, "database").is_err()
            || validate_storage_name(&rec.table, "table").is_err()
        {
            return Ok(false);
        }
        let key = TableKey {
            db: rec.db.clone(),
            table: rec.table.clone(),
        };
        if self
            .get_schema(&rec.db, &rec.table)
            .map(|schema| schema.incarnation_id != rec.incarnation_id)
            .unwrap_or(true)
        {
            return Ok(false);
        }
        // Never resurrect rows into a table that is locked for a missing encryption key.
        if self.encrypted_locked_tables.contains(&key) {
            return Ok(false);
        }
        // Row versions are the monotonic seed used by later mutations. Restore the durable
        // schema version and generated-key sequence even when the snapshot already contains
        // this row and the redo itself is therefore an idempotent no-op.
        let schema = self.get_schema_mut(&rec.db, &rec.table)?;
        schema.table_version = schema.table_version.max(rec.version);
        if let Some(auto_inc_next) = rec.auto_inc_next.as_ref() {
            for (column, next) in auto_inc_next {
                let current = schema.auto_inc_next.entry(column.clone()).or_insert(*next);
                *current = (*current).max(*next);
            }
        } else {
            for column in schema.columns.iter().filter(|column| column.auto_increment) {
                if let Some(next) = rec
                    .row
                    .get(&column.name)
                    .and_then(lit_to_u64)
                    .map(|value| value.saturating_add(1))
                {
                    let current = schema
                        .auto_inc_next
                        .entry(column.name.clone())
                        .or_insert(next);
                    *current = (*current).max(next);
                }
            }
        }
        // A streaming table holds no in-memory rows; bring it resident before replaying a
        // recovered mutation onto it, so the row lands in memory and the follow-up persist
        // writes the real (non-empty) image rather than being skipped.
        self.materialize_streaming_table(&key)?;
        let Some(tdata) = self.tables.get_mut(&key) else {
            anyhow::bail!(
                "WAL recovery table data is unavailable: {}.{}",
                rec.db,
                rec.table
            );
        };
        let entry = RowEntry {
            row: rec.row.clone(),
            version: rec.version,
            schema_version: rec.schema_version,
            deleted: rec.deleted,
            commit_ts_ms: rec.commit_ts_ms,
        };
        let pk_s = pk_key(&rec.pk);
        if let Some(&idx) = tdata.pk_index.get(&pk_s) {
            if tdata.rows[idx].version >= entry.version {
                return Ok(false); // snapshot already holds this version or newer
            }
            tdata.rows[idx] = entry;
        } else {
            let idx = tdata.rows.len();
            tdata.rows.push(entry);
            tdata.pk_index.insert(pk_s, idx);
        }
        Ok(true)
    }

    /// Drop and delete every database's WAL (and any legacy global WAL), discarding all
    /// records. Called after a full checkpoint (or after replay) has made every table
    /// snapshot durable.
    pub(crate) fn wal_truncate(&mut self) {
        // A global truncate would also erase committed records for a table that could not
        // yet be replayed (for example, encrypted data awaiting its key). Keep all logs until
        // a later recovery pass proves every committed record is durable or obsolete.
        if self.wal_recovery_incomplete {
            return;
        }
        // Drop the writers first so their files can be removed, and because nothing is left
        // pending an fsync (the snapshots that superseded them were persisted durably before
        // truncation). Clearing the map resets every database's unsynced-commit counter.
        self.wals.clear();
        // Remove the whole per-database WAL tree and the legacy global WAL. Best-effort: a
        // missing path is already the desired end state. This also covers WALs that were only
        // read during recovery (no in-memory writer was ever opened for them).
        let _ = fs::remove_dir_all(self.wal_dir());
        let _ = fs::remove_file(self.legacy_wal_path());
    }

    /// Record a committed row mutation's snapshot write as deferred: the WAL already made
    /// it durable, so the full-table snapshot rewrite is batched until the next flush
    /// instead of running on every mutation. Tables that are encryption-locked or corrupt
    /// are routed straight to `persist_table`, which correctly refuses the write (the only
    /// way those states reject a mutation), preserving the pre-deferral behavior.
    pub(crate) fn persist_table_deferred(&mut self, db: &str, table: &str) -> anyhow::Result<()> {
        let key = TableKey {
            db: db.to_string(),
            table: table.to_string(),
        };
        if self.encrypted_locked_tables.contains(&key) || self.corrupt_tables.contains(&key) {
            return self.persist_table(db, table);
        }
        self.dirty_tables.insert(key);
        self.mutations_since_flush = self.mutations_since_flush.saturating_add(1);
        if self.mutations_since_flush >= WAL_FLUSH_THRESHOLD {
            self.flush_dirty_tables()?;
        }
        Ok(())
    }

    /// Persist every dirty table's snapshot and the catalog checkpoint, then truncate the
    /// WAL. The catalog contains table-version and auto-increment state carried by row redo,
    /// so it must be durable before those records can be discarded. If a persist fails the
    /// error propagates with the dirty tables retained and the WAL left intact.
    pub(crate) fn flush_dirty_tables(&mut self) -> anyhow::Result<()> {
        if self.dirty_tables.is_empty() {
            self.mutations_since_flush = 0;
            return Ok(());
        }
        let targets: Vec<TableKey> = self.dirty_tables.iter().cloned().collect();
        for key in &targets {
            self.persist_table(&key.db, &key.table)?;
        }
        self.persist_catalog()?;
        for key in &targets {
            self.dirty_tables.remove(key);
        }
        // Compact the append-only CDC + forensic sidecars into their snapshots and truncate
        // them, bounding sidecar size + recovery replay to one flush interval.
        self.compact_change_forensic_logs();
        self.mutations_since_flush = 0;
        self.wal_truncate();
        Ok(())
    }
}
