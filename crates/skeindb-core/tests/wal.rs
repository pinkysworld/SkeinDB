use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use skeindb_core::wal::{WalReader, WalWriter};

fn temp_path(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.push(format!(
        "skeindb_core_wal_it_{}_{}_{}",
        label,
        std::process::id(),
        ts
    ));
    path
}

#[test]
fn wal_writer_reader_roundtrip_from_file() -> Result<(), Box<dyn std::error::Error>> {
    let path = temp_path("roundtrip");
    {
        let mut writer = WalWriter::open(&path)?;
        writer.begin_txn(10)?;
        writer.append_mutation(10, b"alpha".to_vec())?;
        writer.append_mutation(10, b"beta".to_vec())?;
        writer.commit_txn(10)?;
        writer.sync()?;
    }

    let records = WalReader::open(&path)?.read_all()?;
    assert_eq!(records.len(), 4);
    assert_eq!(records[0].lsn(), 1);
    assert_eq!(records[3].lsn(), 4);

    let recovery = WalReader::open(&path)?.recover()?;
    assert_eq!(recovery.txns.len(), 1);
    assert_eq!(recovery.txns[0].txn_id, 10);
    assert_eq!(recovery.txns[0].mutations.len(), 2);
    assert_eq!(recovery.txns[0].mutations[0].payload, b"alpha");
    assert_eq!(recovery.txns[0].mutations[1].payload, b"beta");

    let _ = std::fs::remove_file(path);
    Ok(())
}

#[test]
fn wal_recovery_tolerates_truncated_tail() -> Result<(), Box<dyn std::error::Error>> {
    let path = temp_path("truncated_tail");
    {
        let mut writer = WalWriter::open(&path)?;
        writer.begin_txn(1)?;
        writer.append_mutation(1, b"ok".to_vec())?;
        writer.commit_txn(1)?;
        writer.begin_txn(2)?;
        writer.append_mutation(2, b"partial".to_vec())?;
        writer.sync()?;
    }
    let bytes = std::fs::read(&path)?;
    std::fs::write(&path, &bytes[..bytes.len() - 5])?;

    let recovery = WalReader::open(&path)?.recover()?;
    assert!(recovery.truncated_tail);
    assert_eq!(recovery.txns.len(), 1);
    assert_eq!(recovery.txns[0].txn_id, 1);
    assert_eq!(recovery.txns[0].mutations.len(), 1);
    assert_eq!(recovery.txns[0].mutations[0].payload, b"ok");

    let _ = std::fs::remove_file(path);
    Ok(())
}
