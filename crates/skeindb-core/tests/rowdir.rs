use skeindb_core::rowdir::{RowDir, RowDirEntry};
use skeindb_core::rowseg::FilePtr;
use std::path::PathBuf;

struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn temp_path(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "skeindb-rowdir-{}-{}-{}.run",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    p
}

#[test]
fn rowdir_flush_and_load_preserves_live_entries_in_row_id_order() {
    let path = temp_path("live");
    let _g = Cleanup(path.clone());

    let mut dir = RowDir::new();
    dir.put(10, FilePtr::new(1, 100));
    dir.put(1, FilePtr::new(1, 200));
    dir.put(42, FilePtr::new(2, 4096));
    dir.put(5, FilePtr::new(1, 300));
    dir.flush_to_run(&path).expect("flush");

    let loaded = RowDir::from_run(&path).expect("load");
    assert_eq!(loaded.len(), 4);
    assert_eq!(loaded.get(1), Some(FilePtr::new(1, 200)));
    assert_eq!(loaded.get(5), Some(FilePtr::new(1, 300)));
    assert_eq!(loaded.get(10), Some(FilePtr::new(1, 100)));
    assert_eq!(loaded.get(42), Some(FilePtr::new(2, 4096)));

    // Verify iteration order is ascending row_id.
    let ids: Vec<u64> = loaded.iter().map(|(k, _)| k).collect();
    assert_eq!(ids, vec![1, 5, 10, 42]);
}

#[test]
fn rowdir_tombstone_in_newer_run_shadows_live_in_older_run() {
    let old_path = temp_path("old");
    let new_path = temp_path("new");
    let _g1 = Cleanup(old_path.clone());
    let _g2 = Cleanup(new_path.clone());

    // Older generation: row_id 1 is live at (1, 64), row_id 2 is live at (1, 128).
    let mut older = RowDir::new();
    older.put(1, FilePtr::new(1, 64));
    older.put(2, FilePtr::new(1, 128));
    older.flush_to_run(&old_path).unwrap();

    // Newer generation: row_id 1 was deleted -> tombstone; row_id 3 appears.
    let mut newer = RowDir::new();
    newer.remove(1);
    newer.put(3, FilePtr::new(2, 256));
    newer.flush_to_run(&new_path).unwrap();

    // Load older first, then newer on top. The tombstone should *remove*
    // the older live entry for row_id=1.
    let mut merged = RowDir::from_run(&old_path).unwrap();
    merged.load_from_run(&new_path).unwrap();

    assert_eq!(merged.get(1), None);
    assert_eq!(
        merged.get_entry(1),
        None,
        "tombstone should have removed older live entry"
    );
    assert_eq!(merged.get(2), Some(FilePtr::new(1, 128)));
    assert_eq!(merged.get(3), Some(FilePtr::new(2, 256)));
}

#[test]
fn rowdir_live_entry_in_newer_run_overwrites_older() {
    let old_path = temp_path("old2");
    let new_path = temp_path("new2");
    let _g1 = Cleanup(old_path.clone());
    let _g2 = Cleanup(new_path.clone());

    let mut older = RowDir::new();
    older.put(7, FilePtr::new(1, 100));
    older.flush_to_run(&old_path).unwrap();

    let mut newer = RowDir::new();
    newer.put(7, FilePtr::new(9, 9000));
    newer.flush_to_run(&new_path).unwrap();

    let mut merged = RowDir::from_run(&old_path).unwrap();
    merged.load_from_run(&new_path).unwrap();
    assert_eq!(merged.get(7), Some(FilePtr::new(9, 9000)));
}

#[test]
fn rowdir_flush_empty_and_reload() {
    let path = temp_path("empty");
    let _g = Cleanup(path.clone());

    let dir = RowDir::new();
    dir.flush_to_run(&path).expect("flush empty");
    let loaded = RowDir::from_run(&path).expect("load empty");
    assert!(loaded.is_empty());
}

#[test]
fn rowdir_roundtrip_with_only_tombstones() {
    let path = temp_path("tombs");
    let _g = Cleanup(path.clone());

    let mut dir = RowDir::new();
    dir.remove(100);
    dir.remove(200);
    assert_eq!(dir.get_entry(100), Some(RowDirEntry::Tombstone));
    dir.flush_to_run(&path).unwrap();

    let mut other = RowDir::new();
    other.put(100, FilePtr::new(1, 1));
    other.put(200, FilePtr::new(1, 2));
    other.put(300, FilePtr::new(1, 3));
    other.load_from_run(&path).unwrap();
    assert_eq!(other.get(100), None);
    assert_eq!(other.get(200), None);
    assert_eq!(other.get(300), Some(FilePtr::new(1, 3)));
}
