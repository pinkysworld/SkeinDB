//! MVCC visibility over RowDir + RowSeg chains (T016).
//!
//! This module resolves the visible row version for a given snapshot timestamp
//! by walking a `RowVersion::prev_ptr` chain until it finds the first version
//! whose `[begin_ts, end_ts)` window covers the snapshot.
//!
//! Visibility rules for the current core format:
//!
//! - `begin_ts == 0` means "staged / not yet committed" and is never visible.
//! - A version is visible at `snapshot_ts` iff:
//!   `begin_ts != 0 && begin_ts <= snapshot_ts && (end_ts == 0 || end_ts > snapshot_ts)`.
//! - `Snapshot::latest()` behaves like reading at `+INF`, so the first visible
//!   head version wins.
//! - If the visible version is a delete marker, the row is considered deleted.
//!
//! Two lookup entry points are provided:
//!
//! - [`resolve_head`] starts from a known `FilePtr` head.
//! - [`resolve_row_id`] starts from `RowDir::get(row_id)` and therefore only
//!   works when the caller still has a head pointer for that row.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use thiserror::Error;

use crate::rowdir::RowDir;
use crate::rowseg::{FilePtr, RowSegError, RowSegmentReader, RowVersion};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    as_of_ts: Option<u64>,
}

impl Snapshot {
    pub fn latest() -> Self {
        Self { as_of_ts: None }
    }

    pub fn as_of(ts: u64) -> Self {
        Self { as_of_ts: Some(ts) }
    }

    pub fn timestamp(self) -> Option<u64> {
        self.as_of_ts
    }

    pub fn is_row_visible(self, row: &RowVersion) -> bool {
        if row.begin_ts == 0 {
            return false;
        }
        let snapshot_ts = self.as_of_ts.unwrap_or(u64::MAX);
        row.begin_ts <= snapshot_ts && (row.end_ts == 0 || row.end_ts > snapshot_ts)
    }
}

#[derive(Debug, Error)]
pub enum MvccError {
    #[error("missing row segment for file_id {0}")]
    MissingSegment(u32),
    #[error("MVCC chain cycle detected at {ptr:?}")]
    ChainCycle { ptr: FilePtr },
    #[error("MVCC chain row_id mismatch (expected {expected}, got {actual})")]
    RowIdMismatch { expected: u64, actual: u64 },
    #[error("MVCC chain table_id mismatch (expected {expected}, got {actual})")]
    TableIdMismatch { expected: u32, actual: u32 },
    #[error(transparent)]
    RowSeg(#[from] RowSegError),
}

pub trait RowVersionResolver {
    fn load_row_version(&mut self, ptr: FilePtr) -> Result<RowVersion, MvccError>;
}

#[derive(Default)]
pub struct RowSegmentSet {
    readers: HashMap<u32, RowSegmentReader>,
}

impl RowSegmentSet {
    pub fn new() -> Self {
        Self {
            readers: HashMap::new(),
        }
    }

    pub fn open_paths<I, P>(paths: I) -> Result<Self, MvccError>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        let mut set = Self::new();
        for path in paths {
            set.insert_path(path)?;
        }
        Ok(set)
    }

    pub fn insert_path<P: AsRef<Path>>(&mut self, path: P) -> Result<(), MvccError> {
        let reader = RowSegmentReader::open(path)?;
        self.readers.insert(reader.file_id(), reader);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.readers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.readers.is_empty()
    }
}

impl RowVersionResolver for RowSegmentSet {
    fn load_row_version(&mut self, ptr: FilePtr) -> Result<RowVersion, MvccError> {
        let reader = self
            .readers
            .get_mut(&ptr.file_id)
            .ok_or(MvccError::MissingSegment(ptr.file_id))?;
        Ok(reader.read_at(ptr.offset)?.row)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleVersion {
    pub ptr: FilePtr,
    pub row: RowVersion,
    pub hops: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccLookup {
    Missing,
    Deleted(VisibleVersion),
    Visible(VisibleVersion),
}

impl MvccLookup {
    pub fn visible(self) -> Option<VisibleVersion> {
        match self {
            Self::Visible(version) => Some(version),
            Self::Missing | Self::Deleted(_) => None,
        }
    }

    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }
}

pub fn resolve_head<R: RowVersionResolver>(
    resolver: &mut R,
    head: FilePtr,
    snapshot: Snapshot,
) -> Result<MvccLookup, MvccError> {
    resolve_chain(resolver, head, None, None, snapshot)
}

pub fn resolve_row_id<R: RowVersionResolver>(
    row_dir: &RowDir,
    resolver: &mut R,
    row_id: u64,
    snapshot: Snapshot,
) -> Result<MvccLookup, MvccError> {
    let Some(head) = row_dir.get(row_id) else {
        return Ok(MvccLookup::Missing);
    };
    resolve_chain(resolver, head, Some(row_id), None, snapshot)
}

fn resolve_chain<R: RowVersionResolver>(
    resolver: &mut R,
    mut ptr: FilePtr,
    mut expected_row_id: Option<u64>,
    mut expected_table_id: Option<u32>,
    snapshot: Snapshot,
) -> Result<MvccLookup, MvccError> {
    if ptr.is_none() {
        return Ok(MvccLookup::Missing);
    }

    let mut seen = HashSet::new();
    let mut hops = 0;
    loop {
        if !seen.insert(ptr) {
            return Err(MvccError::ChainCycle { ptr });
        }

        let row = resolver.load_row_version(ptr)?;
        if let Some(expected) = expected_row_id {
            if row.row_id != expected {
                return Err(MvccError::RowIdMismatch {
                    expected,
                    actual: row.row_id,
                });
            }
        } else {
            expected_row_id = Some(row.row_id);
        }
        if let Some(expected) = expected_table_id {
            if row.table_id != expected {
                return Err(MvccError::TableIdMismatch {
                    expected,
                    actual: row.table_id,
                });
            }
        } else {
            expected_table_id = Some(row.table_id);
        }

        hops += 1;
        if snapshot.is_row_visible(&row) {
            let version = VisibleVersion { ptr, row, hops };
            return Ok(if version.row.is_delete() {
                MvccLookup::Deleted(version)
            } else {
                MvccLookup::Visible(version)
            });
        }

        if row.prev_ptr.is_none() {
            return Ok(MvccLookup::Missing);
        }
        ptr = row.prev_ptr;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::rowseg::{RowGroup, RowGroupRef};

    #[derive(Default)]
    struct FakeResolver {
        rows: HashMap<FilePtr, RowVersion>,
    }

    impl FakeResolver {
        fn insert(&mut self, ptr: FilePtr, row: RowVersion) {
            self.rows.insert(ptr, row);
        }
    }

    impl RowVersionResolver for FakeResolver {
        fn load_row_version(&mut self, ptr: FilePtr) -> Result<RowVersion, MvccError> {
            Ok(self.rows.get(&ptr).cloned().expect("missing fake row"))
        }
    }

    fn row(
        table_id: u32,
        row_id: u64,
        begin_ts: u64,
        end_ts: u64,
        prev_ptr: FilePtr,
        is_delete: bool,
    ) -> RowVersion {
        RowVersion::new(
            table_id,
            row_id,
            begin_ts,
            vec![RowGroup {
                group_id: 0,
                data: RowGroupRef::Inline(b"v".to_vec()),
            }],
        )
        .with_prev(prev_ptr)
        .with_end_ts(end_ts)
        .with_is_delete(is_delete)
    }

    #[test]
    fn snapshot_visibility_windows_match_begin_end_rules() {
        let live = row(1, 7, 100, 200, FilePtr::NONE, false);
        assert!(!Snapshot::latest().is_row_visible(&row(1, 7, 0, 0, FilePtr::NONE, false)));
        assert!(!Snapshot::as_of(99).is_row_visible(&live));
        assert!(Snapshot::as_of(100).is_row_visible(&live));
        assert!(Snapshot::as_of(150).is_row_visible(&live));
        assert!(!Snapshot::as_of(200).is_row_visible(&live));
    }

    #[test]
    fn resolve_head_returns_deleted_for_visible_delete_marker() {
        let p1 = FilePtr::new(1, 64);
        let p2 = FilePtr::new(1, 128);
        let mut resolver = FakeResolver::default();
        resolver.insert(p1, row(3, 42, 100, 200, FilePtr::NONE, false));
        resolver.insert(p2, row(3, 42, 200, 0, p1, true));

        let lookup = resolve_head(&mut resolver, p2, Snapshot::latest()).unwrap();
        match lookup {
            MvccLookup::Deleted(version) => {
                assert_eq!(version.ptr, p2);
                assert_eq!(version.row.begin_ts, 200);
                assert_eq!(version.hops, 1);
            }
            other => panic!("expected Deleted, got {other:?}"),
        }
    }

    #[test]
    fn resolve_head_walks_past_newer_versions_for_historical_snapshot() {
        let p1 = FilePtr::new(1, 64);
        let p2 = FilePtr::new(1, 128);
        let p3 = FilePtr::new(1, 192);
        let mut resolver = FakeResolver::default();
        resolver.insert(p1, row(3, 42, 100, 200, FilePtr::NONE, false));
        resolver.insert(p2, row(3, 42, 200, 300, p1, false));
        resolver.insert(p3, row(3, 42, 300, 0, p2, true));

        let lookup = resolve_head(&mut resolver, p3, Snapshot::as_of(250)).unwrap();
        match lookup {
            MvccLookup::Visible(version) => {
                assert_eq!(version.ptr, p2);
                assert_eq!(version.row.begin_ts, 200);
                assert_eq!(version.hops, 2);
            }
            other => panic!("expected Visible, got {other:?}"),
        }
    }

    #[test]
    fn resolve_head_skips_staged_begin_ts_zero_and_falls_back() {
        let p1 = FilePtr::new(1, 64);
        let p2 = FilePtr::new(1, 128);
        let mut resolver = FakeResolver::default();
        resolver.insert(p1, row(3, 42, 100, 0, FilePtr::NONE, false));
        resolver.insert(p2, row(3, 42, 0, 0, p1, false));

        let lookup = resolve_head(&mut resolver, p2, Snapshot::latest()).unwrap();
        match lookup {
            MvccLookup::Visible(version) => {
                assert_eq!(version.ptr, p1);
                assert_eq!(version.row.begin_ts, 100);
                assert_eq!(version.hops, 2);
            }
            other => panic!("expected Visible, got {other:?}"),
        }
    }

    #[test]
    fn resolve_head_detects_cycle() {
        let p1 = FilePtr::new(1, 64);
        let mut resolver = FakeResolver::default();
        resolver.insert(p1, row(3, 42, 0, 0, p1, false));

        let err = resolve_head(&mut resolver, p1, Snapshot::latest()).unwrap_err();
        assert!(matches!(err, MvccError::ChainCycle { ptr } if ptr == p1));
    }

    #[test]
    fn resolve_head_detects_row_id_mismatch() {
        let p1 = FilePtr::new(1, 64);
        let p2 = FilePtr::new(1, 128);
        let mut resolver = FakeResolver::default();
        resolver.insert(p1, row(3, 7, 100, 0, FilePtr::NONE, false));
        resolver.insert(p2, row(3, 8, 0, 0, p1, false));

        let err = resolve_head(&mut resolver, p2, Snapshot::latest()).unwrap_err();
        assert!(matches!(
            err,
            MvccError::RowIdMismatch {
                expected: 8,
                actual: 7
            }
        ));
    }
}
