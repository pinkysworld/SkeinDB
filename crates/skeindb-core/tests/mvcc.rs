use std::path::PathBuf;

use skeindb_core::mvcc::{
    resolve_head, resolve_row_id, MvccError, MvccLookup, RowSegmentSet, RowVersionResolver,
    Snapshot, VisibleVersionIndex,
};
use skeindb_core::rowdir::RowDir;
use skeindb_core::rowseg::{FilePtr, RowGroup, RowGroupRef, RowSegmentWriter, RowVersion};

struct CountingSegments {
    inner: RowSegmentSet,
    loads: usize,
}

impl CountingSegments {
    fn open_paths<I, P>(paths: I) -> Result<Self, MvccError>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<std::path::Path>,
    {
        Ok(Self {
            inner: RowSegmentSet::open_paths(paths)?,
            loads: 0,
        })
    }
}

impl RowVersionResolver for CountingSegments {
    fn load_row_version(&mut self, ptr: FilePtr) -> Result<RowVersion, MvccError> {
        self.loads += 1;
        self.inner.load_row_version(ptr)
    }
}

struct Cleanup(Vec<PathBuf>);

impl Drop for Cleanup {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn temp_path(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "skeindb-mvcc-{}-{}-{}.rseg",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    p
}

fn row(
    table_id: u32,
    row_id: u64,
    begin_ts: u64,
    end_ts: u64,
    prev_ptr: FilePtr,
    is_delete: bool,
    body: &[u8],
) -> RowVersion {
    RowVersion::new(
        table_id,
        row_id,
        begin_ts,
        vec![RowGroup {
            group_id: 0,
            data: RowGroupRef::Inline(body.to_vec()),
        }],
    )
    .with_prev(prev_ptr)
    .with_end_ts(end_ts)
    .with_is_delete(is_delete)
}

#[test]
fn mvcc_resolve_head_across_delete_marker_and_history() {
    let path = temp_path("history");
    let _cleanup = Cleanup(vec![path.clone()]);

    let mut writer = RowSegmentWriter::create(&path, 11, 1_700_000_000).unwrap();
    let p1 = writer
        .append(&row(7, 42, 100, 200, FilePtr::NONE, false, b"v1"))
        .unwrap();
    let p2 = writer
        .append(&row(7, 42, 200, 300, p1, false, b"v2"))
        .unwrap();
    let p3 = writer
        .append(&row(7, 42, 300, 0, p2, true, b"delete"))
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let mut segments = RowSegmentSet::open_paths([path.as_path()]).unwrap();

    let latest = resolve_head(&mut segments, p3, Snapshot::latest()).unwrap();
    assert!(matches!(latest, MvccLookup::Deleted(_)));

    let at_250 = resolve_head(&mut segments, p3, Snapshot::as_of(250)).unwrap();
    match at_250 {
        MvccLookup::Visible(version) => {
            assert_eq!(version.ptr, p2);
            assert_eq!(version.row.begin_ts, 200);
        }
        other => panic!("expected Visible at 250, got {other:?}"),
    }

    let at_150 = resolve_head(&mut segments, p3, Snapshot::as_of(150)).unwrap();
    match at_150 {
        MvccLookup::Visible(version) => {
            assert_eq!(version.ptr, p1);
            assert_eq!(version.row.begin_ts, 100);
        }
        other => panic!("expected Visible at 150, got {other:?}"),
    }

    assert!(resolve_head(&mut segments, p3, Snapshot::as_of(50))
        .unwrap()
        .is_missing());
}

#[test]
fn mvcc_resolve_row_id_uses_rowdir_head_across_multiple_segments() {
    let path1 = temp_path("seg1");
    let path2 = temp_path("seg2");
    let _cleanup = Cleanup(vec![path1.clone(), path2.clone()]);

    let mut writer1 = RowSegmentWriter::create(&path1, 21, 1_700_000_000).unwrap();
    let p1 = writer1
        .append(&row(9, 7, 100, 200, FilePtr::NONE, false, b"old"))
        .unwrap();
    writer1.sync().unwrap();
    drop(writer1);

    let mut writer2 = RowSegmentWriter::create(&path2, 22, 1_700_000_000).unwrap();
    let p2 = writer2
        .append(&row(9, 7, 200, 0, p1, false, b"new"))
        .unwrap();
    writer2.sync().unwrap();
    drop(writer2);

    let mut row_dir = RowDir::new();
    row_dir.put(7, p2);

    let mut segments = RowSegmentSet::open_paths([path1.as_path(), path2.as_path()]).unwrap();
    assert_eq!(segments.len(), 2);

    let latest = resolve_row_id(&row_dir, &mut segments, 7, Snapshot::latest()).unwrap();
    match latest {
        MvccLookup::Visible(version) => {
            assert_eq!(version.ptr, p2);
            assert_eq!(version.row.begin_ts, 200);
        }
        other => panic!("expected Visible latest, got {other:?}"),
    }

    let at_150 = resolve_row_id(&row_dir, &mut segments, 7, Snapshot::as_of(150)).unwrap();
    match at_150 {
        MvccLookup::Visible(version) => {
            assert_eq!(version.ptr, p1);
            assert_eq!(version.row.begin_ts, 100);
        }
        other => panic!("expected Visible at 150, got {other:?}"),
    }

    assert!(
        resolve_row_id(&row_dir, &mut segments, 999, Snapshot::latest())
            .unwrap()
            .is_missing()
    );
}

#[test]
fn mvcc_skips_staged_head_and_keeps_previous_committed_version_visible() {
    let path = temp_path("staged");
    let _cleanup = Cleanup(vec![path.clone()]);

    let mut writer = RowSegmentWriter::create(&path, 31, 1_700_000_000).unwrap();
    let p1 = writer
        .append(&row(5, 99, 100, 0, FilePtr::NONE, false, b"committed"))
        .unwrap();
    let p2 = writer
        .append(&row(5, 99, 0, 0, p1, false, b"staged"))
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let mut segments = RowSegmentSet::open_paths([path.as_path()]).unwrap();
    let latest = resolve_head(&mut segments, p2, Snapshot::latest()).unwrap();
    match latest {
        MvccLookup::Visible(version) => {
            assert_eq!(version.ptr, p1);
            assert_eq!(version.row.begin_ts, 100);
            assert_eq!(version.hops, 2);
        }
        other => panic!("expected committed fallback, got {other:?}"),
    }
}

#[test]
fn mvcc_visible_version_index_reuses_file_backed_history_lookup() {
    let path = temp_path("visible-index");
    let _cleanup = Cleanup(vec![path.clone()]);

    let mut writer = RowSegmentWriter::create(&path, 41, 1_700_000_000).unwrap();
    let p1 = writer
        .append(&row(7, 42, 100, 200, FilePtr::NONE, false, b"v1"))
        .unwrap();
    let p2 = writer
        .append(&row(7, 42, 200, 300, p1, false, b"v2"))
        .unwrap();
    let p3 = writer
        .append(&row(7, 42, 300, 0, p2, false, b"v3"))
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let mut row_dir = RowDir::new();
    row_dir.put(42, p3);

    let mut segments = CountingSegments::open_paths([path.as_path()]).unwrap();
    let mut index = VisibleVersionIndex::new(100, 8);

    let first = index
        .resolve_row_id(&row_dir, &mut segments, 42, Snapshot::as_of(250))
        .unwrap();
    let second = index
        .resolve_row_id(&row_dir, &mut segments, 42, Snapshot::as_of(299))
        .unwrap();

    match first {
        MvccLookup::Visible(version) => {
            assert_eq!(version.ptr, p2);
            assert_eq!(version.hops, 2);
        }
        other => panic!("expected Visible at 250, got {other:?}"),
    }

    match second {
        MvccLookup::Visible(version) => {
            assert_eq!(version.ptr, p2);
            assert_eq!(version.hops, 2);
        }
        other => panic!("expected cached Visible at 299, got {other:?}"),
    }

    assert_eq!(segments.loads, 2);
    assert_eq!(index.stats().hits, 1);
    assert_eq!(index.stats().misses, 1);
}
