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
        for (db, table) in &touched {
            // Rebuild pk/secondary indexes from the recovered rows, then re-persist.
            let _ = self.rebuild_indexes_after_history_gc(db, table);
            let _ = self.persist_table(db, table);
        }
        self.wal_truncate();
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
}
