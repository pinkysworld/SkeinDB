use skeindb_core::rowseg::{
    FilePtr, RowGroup, RowGroupRef, RowSegError, RowSegmentReader, RowSegmentWriter, RowVersion,
    VALUE_ID_LEN,
};
use std::path::PathBuf;

fn temp_path(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "skeindb-rowseg-{}-{}-{}.rseg",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    p
}

fn row(table_id: u32, row_id: u64, begin_ts: u64, body: &[u8]) -> RowVersion {
    RowVersion::new(
        table_id,
        row_id,
        begin_ts,
        vec![RowGroup {
            group_id: 0,
            data: RowGroupRef::Inline(body.to_vec()),
        }],
    )
}

#[test]
fn rseg_writer_reader_roundtrip_chains_prev_ptrs() {
    let path = temp_path("chain");
    let _guard = scopeguard_remove(path.clone());

    let mut writer = RowSegmentWriter::create(&path, 3, 1_700_000_000).expect("create");

    let v1_row = row(7, 42, 1_700_000_100, b"v1");
    let v1_ptr = writer.append(&v1_row).expect("append v1");
    assert_eq!(v1_ptr.file_id, 3);

    let v2_row = row(7, 42, 1_700_000_200, b"v2").with_prev(v1_ptr);
    let v2_ptr = writer.append(&v2_row).expect("append v2");
    assert!(v2_ptr.offset > v1_ptr.offset);

    let del_row = row(7, 42, 1_700_000_300, b"")
        .with_prev(v2_ptr)
        .with_is_delete(true);
    let del_ptr = writer.append(&del_row).expect("append delete");
    writer.sync().expect("sync");
    drop(writer);

    let mut reader = RowSegmentReader::open(&path).expect("reader");
    assert_eq!(reader.file_id(), 3);
    let all = reader.read_all().expect("read_all");
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].offset, v1_ptr.offset);
    assert_eq!(all[1].offset, v2_ptr.offset);
    assert_eq!(all[2].offset, del_ptr.offset);
    assert_eq!(all[0].row.prev_ptr, FilePtr::NONE);
    assert_eq!(all[1].row.prev_ptr, v1_ptr);
    assert_eq!(all[2].row.prev_ptr, v2_ptr);
    assert!(all[2].row.is_delete());

    // Random access at recorded offsets.
    let fetched = reader.read_at(v2_ptr.offset).expect("read_at v2");
    assert_eq!(fetched.row.begin_ts, 1_700_000_200);
}

#[test]
fn rseg_writer_open_appends_after_reopen() {
    let path = temp_path("reopen");
    let _guard = scopeguard_remove(path.clone());

    let mut writer = RowSegmentWriter::create(&path, 9, 1_700_000_000).expect("create");
    let p1 = writer.append(&row(1, 1, 1_700_000_000, b"one")).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let mut writer = RowSegmentWriter::open(&path).expect("reopen");
    assert_eq!(writer.file_id(), 9);
    let p2 = writer.append(&row(1, 2, 1_700_000_001, b"two")).unwrap();
    writer.sync().unwrap();
    assert!(p2.offset > p1.offset);
    drop(writer);

    let mut reader = RowSegmentReader::open(&path).expect("reader");
    let all = reader.read_all().expect("read_all");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].row.row_id, 1);
    assert_eq!(all[1].row.row_id, 2);
}

#[test]
fn rseg_reader_rejects_wrong_file_kind() {
    // Valuestore header -> reader should reject.
    let path = temp_path("wrongkind");
    let _guard = scopeguard_remove(path.clone());
    // Write a WAL header; it is another valid FileKind but not RowSeg.
    use std::io::Write;
    let mut f = std::fs::File::create(&path).unwrap();
    let hdr = skeindb_core::FileHeader::new(skeindb_core::FileKind::Wal, 1, 0).encode();
    f.write_all(&hdr).unwrap();
    f.sync_all().unwrap();
    drop(f);

    match RowSegmentReader::open(&path) {
        Ok(_) => panic!("expected BadHeader"),
        Err(RowSegError::BadHeader) => {}
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn rseg_supports_value_id_group_refs() {
    let path = temp_path("vidref");
    let _guard = scopeguard_remove(path.clone());

    let mut writer = RowSegmentWriter::create(&path, 5, 1_700_000_000).unwrap();
    let row = RowVersion::new(
        2,
        1,
        1_700_000_000,
        vec![
            RowGroup {
                group_id: 0,
                data: RowGroupRef::Inline(b"inline".to_vec()),
            },
            RowGroup {
                group_id: 1,
                data: RowGroupRef::ValueId([0xABu8; VALUE_ID_LEN]),
            },
        ],
    );
    writer.append(&row).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let mut reader = RowSegmentReader::open(&path).unwrap();
    let all = reader.read_all().unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].row.groups.len(), 2);
    match &all[0].row.groups[1].data {
        RowGroupRef::ValueId(id) => assert_eq!(id, &[0xABu8; VALUE_ID_LEN]),
        _ => panic!("expected ValueId ref"),
    }
}

// Helper to delete the temp file on drop even if the test panics.
struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn scopeguard_remove(path: PathBuf) -> Cleanup {
    Cleanup(path)
}
