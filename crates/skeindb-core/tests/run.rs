use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use skeindb_core::run::{RunReader, RunWriter, RunWriterOptions, SimpleLsm, SimpleLsmConfig};

fn temp_path(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.push(format!(
        "skeindb_core_run_it_{}_{}_{}",
        label,
        std::process::id(),
        ts
    ));
    path
}

#[test]
fn run_writer_reader_roundtrip_across_multiple_blocks() -> Result<(), Box<dyn std::error::Error>> {
    let path = temp_path("roundtrip_blocks");
    let metadata = {
        let mut writer = RunWriter::create(
            &path,
            RunWriterOptions {
                target_block_size: 64,
            },
        )?;
        for i in 0..20u8 {
            writer.append(
                format!("k{i:02}").into_bytes(),
                format!("value-{i:02}").into_bytes(),
            )?;
        }
        writer.finish()?
    };

    assert!(metadata.block_count >= 2);
    let reader = RunReader::open(&path)?;
    let rows = reader.read_all()?;
    assert_eq!(rows.len(), 20);
    assert_eq!(reader.get(b"k00")?.as_deref(), Some(&b"value-00"[..]));
    assert_eq!(reader.get(b"k11")?.as_deref(), Some(&b"value-11"[..]));
    assert_eq!(reader.get(b"zzz")?, None);
    assert_eq!(reader.index().len(), metadata.block_count);

    let _ = std::fs::remove_file(path);
    Ok(())
}

#[test]
fn simple_lsm_flush_and_lookup_prefers_newest_level0_run() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = temp_path("lsm_flush_lookup");
    std::fs::create_dir_all(&dir)?;
    let mut lsm = SimpleLsm::open(
        &dir,
        SimpleLsmConfig {
            target_block_size: 64,
            memtable_max_entries: 2,
        },
    )?;

    assert!(lsm.put(b"a".to_vec(), b"v1".to_vec())?.is_none());
    let first_flush = lsm.put(b"b".to_vec(), b"v2".to_vec())?;
    assert!(first_flush.is_some());
    assert_eq!(lsm.level0_run_count(), 1);

    assert!(lsm.put(b"a".to_vec(), b"v3".to_vec())?.is_none());
    let second_flush = lsm.put(b"c".to_vec(), b"v4".to_vec())?;
    assert!(second_flush.is_some());
    assert_eq!(lsm.level0_run_count(), 2);

    assert_eq!(lsm.get(b"a")?.as_deref(), Some(&b"v3"[..]));
    assert_eq!(lsm.get(b"b")?.as_deref(), Some(&b"v2"[..]));
    assert_eq!(lsm.get(b"c")?.as_deref(), Some(&b"v4"[..]));
    assert_eq!(lsm.get(b"missing")?, None);

    std::fs::remove_dir_all(dir).ok();
    Ok(())
}

#[test]
fn simple_lsm_open_discovers_existing_level0_runs() -> Result<(), Box<dyn std::error::Error>> {
    let dir = temp_path("lsm_reopen");
    std::fs::create_dir_all(&dir)?;
    {
        let mut lsm = SimpleLsm::open(
            &dir,
            SimpleLsmConfig {
                target_block_size: 64,
                memtable_max_entries: 10,
            },
        )?;
        lsm.put(b"a".to_vec(), b"one".to_vec())?;
        lsm.put(b"b".to_vec(), b"two".to_vec())?;
        let flushed = lsm.flush_memtable()?;
        assert!(flushed.is_some());
    }

    let reopened = SimpleLsm::open(
        &dir,
        SimpleLsmConfig {
            target_block_size: 64,
            memtable_max_entries: 10,
        },
    )?;
    assert_eq!(reopened.level0_run_count(), 1);
    assert_eq!(reopened.next_run_id(), 2);
    assert_eq!(reopened.get(b"a")?.as_deref(), Some(&b"one"[..]));
    assert_eq!(reopened.get(b"b")?.as_deref(), Some(&b"two"[..]));

    std::fs::remove_dir_all(dir).ok();
    Ok(())
}
