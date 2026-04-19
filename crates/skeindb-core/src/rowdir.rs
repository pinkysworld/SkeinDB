//! RowDir — in-memory `row_id → FilePtr` directory with sorted-run persistence (T015).
//!
//! The RowDir tracks the *head pointer* (latest [`FilePtr`]) of every live
//! row's MVCC version chain inside `.rseg` files. It is the bridge between
//! `row_id` lookups (common in SQL) and the physical row segment containers.
//!
//! This module keeps a minimal, testable surface:
//!
//! - `RowDir` keeps a `BTreeMap<u64, RowDirEntry>` in memory, sorted by
//!   `row_id` so flushes to sorted runs are free.
//! - `put` / `remove` update the head pointer. `remove` leaves a tombstone
//!   so later merges can shadow an older run's entry.
//! - `get` returns the live head pointer (or `None` if missing or tombstoned).
//! - `flush_to_run` emits a sorted `.run` over `FileHeader(FileKind::Run)`
//!   where each key is a 8-byte big-endian `row_id` and the value is either
//!   `[tag=0][FilePtr(12)]` (live) or `[tag=1]` (tombstone).
//! - `load_from_run` replays a previously flushed run into the in-memory
//!   map (tombstones remove the entry, live entries upsert).
//!
//! Storage layout reuses [`crate::run`] verbatim — no new on-disk format.

use std::collections::BTreeMap;
use std::path::Path;

use thiserror::Error;

use crate::rowseg::{FilePtr, FILE_PTR_ENCODED_LEN};
use crate::run::{RunError, RunReader, RunWriter, RunWriterOptions};

/// Value payload tag for live head-pointer entries.
const ENTRY_TAG_LIVE: u8 = 0;
/// Value payload tag for tombstoned rows.
const ENTRY_TAG_TOMBSTONE: u8 = 1;

#[derive(Debug, Error)]
pub enum RowDirError {
    #[error("invalid rowdir run entry: {0}")]
    InvalidRunEntry(&'static str),
    #[error("unsupported rowdir entry tag {0}")]
    UnsupportedEntryTag(u8),
    #[error(transparent)]
    Run(#[from] RunError),
}

/// One entry in the RowDir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowDirEntry {
    /// Row is live; [`FilePtr`] points at the head of its version chain.
    Live(FilePtr),
    /// Row was deleted; kept as a tombstone so the deletion can shadow an
    /// older run during merges.
    Tombstone,
}

impl RowDirEntry {
    pub fn as_live(self) -> Option<FilePtr> {
        match self {
            Self::Live(ptr) => Some(ptr),
            Self::Tombstone => None,
        }
    }
}

/// In-memory row_id → head FilePtr directory.
#[derive(Debug, Default, Clone)]
pub struct RowDir {
    map: BTreeMap<u64, RowDirEntry>,
}

impl RowDir {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    /// Upsert the head pointer for `row_id`.
    pub fn put(&mut self, row_id: u64, head: FilePtr) {
        self.map.insert(row_id, RowDirEntry::Live(head));
    }

    /// Mark `row_id` as deleted. Keeps a tombstone so a later flush can
    /// shadow any older run's live entry for the same row_id.
    pub fn remove(&mut self, row_id: u64) {
        self.map.insert(row_id, RowDirEntry::Tombstone);
    }

    /// Drop the row_id entry entirely (no tombstone). Use [`Self::remove`]
    /// instead for logical deletes.
    pub fn forget(&mut self, row_id: u64) {
        self.map.remove(&row_id);
    }

    /// Return the live head pointer, or `None` if missing or tombstoned.
    pub fn get(&self, row_id: u64) -> Option<FilePtr> {
        self.map.get(&row_id).and_then(|e| e.as_live())
    }

    /// Return the raw entry (including tombstones).
    pub fn get_entry(&self, row_id: u64) -> Option<RowDirEntry> {
        self.map.get(&row_id).copied()
    }

    /// Number of tracked entries (live + tombstones).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Iterate entries in ascending row_id order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, RowDirEntry)> + '_ {
        self.map.iter().map(|(k, v)| (*k, *v))
    }

    /// Flush this RowDir to a sorted `.run` file.
    ///
    /// Keys are encoded as 8-byte big-endian `row_id`, so ascending byte
    /// order matches ascending numeric order. Values encode the live head
    /// pointer or a tombstone tag.
    pub fn flush_to_run<P: AsRef<Path>>(&self, path: P) -> Result<(), RowDirError> {
        let mut writer = RunWriter::create(path, RunWriterOptions::default())?;
        for (row_id, entry) in &self.map {
            let key = row_id.to_be_bytes().to_vec();
            let value = encode_entry(*entry);
            writer.append(key, value)?;
        }
        writer.finish()?;
        Ok(())
    }

    /// Replay a `.run` produced by [`Self::flush_to_run`] into this RowDir.
    ///
    /// Semantics:
    /// - Live entries upsert (overwrite) any existing entry.
    /// - Tombstones remove any existing entry entirely (so the consumer can
    ///   compact older-run state against a newer-run tombstone).
    pub fn load_from_run<P: AsRef<Path>>(&mut self, path: P) -> Result<(), RowDirError> {
        let reader = RunReader::open(path)?;
        for entry in reader.read_all()? {
            let row_id = decode_row_id_key(&entry.key)?;
            match decode_entry(&entry.value)? {
                RowDirEntry::Live(ptr) => {
                    self.map.insert(row_id, RowDirEntry::Live(ptr));
                }
                RowDirEntry::Tombstone => {
                    self.map.remove(&row_id);
                }
            }
        }
        Ok(())
    }

    /// Load a fresh RowDir from a previously flushed `.run` file.
    pub fn from_run<P: AsRef<Path>>(path: P) -> Result<Self, RowDirError> {
        let mut dir = Self::new();
        dir.load_from_run(path)?;
        Ok(dir)
    }
}

fn encode_entry(entry: RowDirEntry) -> Vec<u8> {
    match entry {
        RowDirEntry::Live(ptr) => {
            let mut out = Vec::with_capacity(1 + FILE_PTR_ENCODED_LEN);
            out.push(ENTRY_TAG_LIVE);
            out.extend_from_slice(&ptr.encode());
            out
        }
        RowDirEntry::Tombstone => vec![ENTRY_TAG_TOMBSTONE],
    }
}

fn decode_entry(bytes: &[u8]) -> Result<RowDirEntry, RowDirError> {
    if bytes.is_empty() {
        return Err(RowDirError::InvalidRunEntry("empty value"));
    }
    let tag = bytes[0];
    match tag {
        ENTRY_TAG_LIVE => {
            if bytes.len() != 1 + FILE_PTR_ENCODED_LEN {
                return Err(RowDirError::InvalidRunEntry("live entry length"));
            }
            let ptr = FilePtr::decode(&bytes[1..])
                .map_err(|_| RowDirError::InvalidRunEntry("malformed FilePtr"))?;
            Ok(RowDirEntry::Live(ptr))
        }
        ENTRY_TAG_TOMBSTONE => {
            if bytes.len() != 1 {
                return Err(RowDirError::InvalidRunEntry("tombstone trailing bytes"));
            }
            Ok(RowDirEntry::Tombstone)
        }
        other => Err(RowDirError::UnsupportedEntryTag(other)),
    }
}

fn decode_row_id_key(key: &[u8]) -> Result<u64, RowDirError> {
    if key.len() != 8 {
        return Err(RowDirError::InvalidRunEntry("row_id key length"));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(key);
    Ok(u64::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_remove_and_forget() {
        let mut dir = RowDir::new();
        let p1 = FilePtr::new(1, 64);
        let p2 = FilePtr::new(1, 128);
        dir.put(1, p1);
        dir.put(2, p2);
        assert_eq!(dir.get(1), Some(p1));
        assert_eq!(dir.get(2), Some(p2));
        assert_eq!(dir.len(), 2);

        dir.remove(1);
        assert_eq!(dir.get(1), None);
        assert_eq!(dir.get_entry(1), Some(RowDirEntry::Tombstone));
        assert_eq!(dir.len(), 2);

        dir.forget(1);
        assert_eq!(dir.get_entry(1), None);
        assert_eq!(dir.len(), 1);
    }

    #[test]
    fn iter_is_sorted_by_row_id() {
        let mut dir = RowDir::new();
        dir.put(3, FilePtr::new(1, 0));
        dir.put(1, FilePtr::new(1, 16));
        dir.put(2, FilePtr::new(1, 32));
        let ids: Vec<u64> = dir.iter().map(|(k, _)| k).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn encode_decode_live_and_tombstone() {
        let live = RowDirEntry::Live(FilePtr::new(7, 0xdead_beef));
        let tomb = RowDirEntry::Tombstone;
        assert_eq!(decode_entry(&encode_entry(live)).unwrap(), live);
        assert_eq!(decode_entry(&encode_entry(tomb)).unwrap(), tomb);
    }

    #[test]
    fn decode_rejects_bad_tag_and_length() {
        assert!(matches!(
            decode_entry(&[]).unwrap_err(),
            RowDirError::InvalidRunEntry(_)
        ));
        assert!(matches!(
            decode_entry(&[9u8]).unwrap_err(),
            RowDirError::UnsupportedEntryTag(9)
        ));
        assert!(matches!(
            decode_entry(&[ENTRY_TAG_LIVE, 1, 2]).unwrap_err(),
            RowDirError::InvalidRunEntry(_)
        ));
        assert!(matches!(
            decode_entry(&[ENTRY_TAG_TOMBSTONE, 0]).unwrap_err(),
            RowDirError::InvalidRunEntry(_)
        ));
    }
}
