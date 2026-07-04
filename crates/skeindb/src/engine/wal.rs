//! Row-level redo write-ahead log for crash recovery of table data.
//!
//! Each committed DML mutation (insert/update/delete) appends the full final image of
//! every changed row to a single global WAL (`<data_dir>/wal-000001.log`) and fsyncs it
//! *before* the table snapshot is written. On the next [`Engine::open`] the committed
//! records are replayed onto the loaded snapshots. Redo is idempotent: each record
//! carries the row's full final image plus its monotonic `version` and `commit_ts_ms`,
//! so re-applying a record the snapshot already holds is a no-op (the version check
//! skips it), while a mutation lost between its WAL commit and the snapshot write is
//! recovered. After replay — and at every checkpoint — the recovered tables are
//! re-persisted and the WAL is truncated to a clean state.
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
    pub pk: Vec<Lit>,
    pub row: RowObject,
    pub version: u64,
    pub schema_version: u64,
    pub deleted: bool,
    pub commit_ts_ms: u64,
}

impl WalRowRecord {
    /// Build a redo record from a row entry that was just written in memory.
    pub(crate) fn from_entry(db: &str, table: &str, pk: Vec<Lit>, entry: &RowEntry) -> Self {
        Self {
            db: db.to_string(),
            table: table.to_string(),
            pk,
            row: entry.row.clone(),
            version: entry.version,
            schema_version: entry.schema_version,
            deleted: entry.deleted,
            commit_ts_ms: entry.commit_ts_ms,
        }
    }
}

impl Engine {
    pub(crate) fn wal_path(&self) -> PathBuf {
        self.data_dir.join("wal-000001.log")
    }

    /// Durably append a batch of row redo records as one committed WAL transaction.
    /// Called by the DML handlers *before* they persist the table snapshot, so a crash
    /// after this returns can always recover the mutation on the next open().
    pub(crate) fn wal_log_rows(&mut self, records: &[WalRowRecord]) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if self.wal.is_none() {
            self.wal = Some(WalWriter::open(self.wal_path())?);
        }
        let txn = self.wal_next_txn;
        self.wal_next_txn = self.wal_next_txn.wrapping_add(1);
        let writer = self.wal.as_mut().expect("wal writer opened above");
        writer.begin_txn(txn)?;
        for rec in records {
            writer.append_mutation(txn, serde_json::to_vec(rec)?)?;
        }
        writer.commit_txn(txn)?;
        writer.sync()?;
        Ok(())
    }

    /// Replay committed WAL records after snapshots are loaded on open(). Idempotent;
    /// recovers any mutation that did not reach its table snapshot before a crash, then
    /// re-persists the affected tables and truncates the WAL to a clean checkpoint.
    pub(crate) fn wal_recover(&mut self) {
        let path = self.wal_path();
        if !path.exists() {
            return;
        }
        let Ok(reader) = WalReader::open(&path) else {
            return;
        };
        let Ok(recovery) = reader.recover() else {
            return;
        };
        let mut touched: BTreeSet<(String, String)> = BTreeSet::new();
        for txn in &recovery.txns {
            for mutation in &txn.mutations {
                let Ok(rec) = serde_json::from_slice::<WalRowRecord>(&mutation.payload) else {
                    continue;
                };
                if self.wal_apply_record(&rec) {
                    touched.insert((rec.db, rec.table));
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
        if all_persisted {
            self.wal_truncate();
        }
    }

    /// Apply a single redo record idempotently. Returns true if the in-memory table was
    /// changed (i.e. the record was newer than what the loaded snapshot already held).
    fn wal_apply_record(&mut self, rec: &WalRowRecord) -> bool {
        let key = TableKey {
            db: rec.db.clone(),
            table: rec.table.clone(),
        };
        // Never resurrect rows into a table that is locked for a missing encryption key.
        if self.encrypted_locked_tables.contains(&key) {
            return false;
        }
        // A streaming table holds no in-memory rows; bring it resident before replaying a
        // recovered mutation onto it, so the row lands in memory and the follow-up persist
        // writes the real (non-empty) image rather than being skipped.
        let _ = self.materialize_streaming_table(&key);
        let Some(tdata) = self.tables.get_mut(&key) else {
            return false;
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
                return false; // snapshot already holds this version or newer
            }
            tdata.rows[idx] = entry;
        } else {
            let idx = tdata.rows.len();
            tdata.rows.push(entry);
            tdata.pk_index.insert(pk_s, idx);
        }
        true
    }

    /// Drop and delete the WAL file, discarding all records. Called after a full
    /// checkpoint (or after replay) has made every table snapshot durable.
    pub(crate) fn wal_truncate(&mut self) {
        self.wal = None;
        let _ = fs::remove_file(self.wal_path());
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

    /// Persist every dirty table's snapshot, then truncate the WAL. After this the
    /// snapshots contain everything the WAL held, so the redo log is reset. If a persist
    /// fails the error propagates with the still-dirty tables retained and the WAL left
    /// intact, so nothing is lost (the next flush or `open()` replay recovers it).
    pub(crate) fn flush_dirty_tables(&mut self) -> anyhow::Result<()> {
        if self.dirty_tables.is_empty() {
            self.mutations_since_flush = 0;
            return Ok(());
        }
        let targets: Vec<TableKey> = self.dirty_tables.iter().cloned().collect();
        for key in &targets {
            self.persist_table(&key.db, &key.table)?;
            self.dirty_tables.remove(key);
        }
        self.mutations_since_flush = 0;
        self.wal_truncate();
        Ok(())
    }
}
