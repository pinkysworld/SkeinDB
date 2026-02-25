use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use skeindb_core::{
    append_record_frame, iter_record_frames, FileHeader, FileKind, FormatError, FILE_HEADER_LEN,
};

fn temp_path(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.push(format!(
        "skeindb_core_{}_{}_{}",
        label,
        std::process::id(),
        ts
    ));
    path
}

#[test]
fn file_header_write_read_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let path = temp_path("file_header");
    let header = FileHeader::new(FileKind::Wal, 17, 1_735_690_111);
    let encoded = header.encode();

    std::fs::write(&path, encoded)?;
    let bytes = std::fs::read(&path)?;
    assert_eq!(bytes.len(), FILE_HEADER_LEN);

    let decoded = FileHeader::decode(&bytes)?;
    assert_eq!(decoded, header);

    let _ = std::fs::remove_file(path);
    Ok(())
}

#[test]
fn record_frame_append_iterate_roundtrip_from_file() -> Result<(), Box<dyn std::error::Error>> {
    let path = temp_path("record_frames");
    let mut bytes = Vec::new();
    append_record_frame(&mut bytes, b"alpha")?;
    append_record_frame(&mut bytes, b"beta")?;
    append_record_frame(&mut bytes, b"gamma")?;

    std::fs::write(&path, &bytes)?;
    let on_disk = std::fs::read(&path)?;

    let frames = iter_record_frames(&on_disk).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        frames,
        vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

#[test]
fn record_frame_iterator_reports_crc_corruption() -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    append_record_frame(&mut bytes, b"first")?;
    append_record_frame(&mut bytes, b"second")?;

    // Corrupt the second frame CRC (first frame is 8-byte header + 5-byte payload).
    let second_crc_offset = 13 + 4;
    bytes[second_crc_offset] ^= 0x80;

    let mut it = iter_record_frames(&bytes);
    assert_eq!(it.next().expect("first frame").expect("first ok"), b"first");
    let err = it
        .next()
        .expect("second frame should fail")
        .expect_err("crc");
    assert!(matches!(err, FormatError::RecordFrameCrcMismatch { .. }));
    assert!(it.next().is_none());
    Ok(())
}
