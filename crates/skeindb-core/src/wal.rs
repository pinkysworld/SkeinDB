//! WAL writer/reader + committed-transaction recovery (T011)
//!
//! This module provides a minimal append-only WAL implementation on top of the
//! shared `FileHeader(FileKind::Wal)` and `RecordFrame` primitives. The record
//! layout matches the current `docs/ON_DISK_FORMAT.md` WAL header contract and
//! adds a small v1 body vocabulary for transaction recovery:
//!
//! - `BEGIN_TXN`
//! - `MUTATION` (opaque bytes payload)
//! - `COMMIT_TXN`
//! - `ABORT_TXN`
//!
//! Recovery is intentionally conservative:
//!
//! - only transactions with a valid `COMMIT_TXN` are replayed,
//! - aborted transactions are discarded,
//! - a torn or corrupt tail stops scanning and the remaining tail bytes are
//!   discarded, while all previously-valid records are still recoverable.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{
    append_record_frame, decode_record_frame, decode_varu, encode_varu, FileHeader, FileKind,
    FormatError, WalHeaderV1, WalHeaderV2, WalHeaderVersion, FILE_HEADER_LEN,
};

const WAL_HEADER_V1_LEN: usize = 20;
const WAL_HEADER_V2_LEN: usize = WAL_HEADER_V1_LEN + 32 + 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordKind {
    BeginTxn = 0x01,
    Mutation = 0x02,
    CommitTxn = 0x03,
    AbortTxn = 0x04,
}

impl WalRecordKind {
    fn from_u8(v: u8) -> Result<Self, WalError> {
        match v {
            0x01 => Ok(Self::BeginTxn),
            0x02 => Ok(Self::Mutation),
            0x03 => Ok(Self::CommitTxn),
            0x04 => Ok(Self::AbortTxn),
            _ => Err(WalError::UnknownRecordType(v)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecordBody {
    BeginTxn,
    Mutation { payload: Vec<u8> },
    CommitTxn,
    AbortTxn,
}

impl WalRecordBody {
    fn kind(&self) -> WalRecordKind {
        match self {
            Self::BeginTxn => WalRecordKind::BeginTxn,
            Self::Mutation { .. } => WalRecordKind::Mutation,
            Self::CommitTxn => WalRecordKind::CommitTxn,
            Self::AbortTxn => WalRecordKind::AbortTxn,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecordHeader {
    V1(WalHeaderV1),
    V2(WalHeaderV2),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub header: WalRecordHeader,
    pub body: WalRecordBody,
}

impl WalRecord {
    pub fn begin_v1(txn_id: u64) -> Self {
        Self::new_v1(WalRecordBody::BeginTxn, txn_id, 0, 0)
    }

    pub fn mutation_v1(txn_id: u64, payload: Vec<u8>) -> Self {
        Self::new_v1(WalRecordBody::Mutation { payload }, txn_id, 0, 0)
    }

    pub fn commit_v1(txn_id: u64) -> Self {
        Self::new_v1(WalRecordBody::CommitTxn, txn_id, 0, 0)
    }

    pub fn abort_v1(txn_id: u64) -> Self {
        Self::new_v1(WalRecordBody::AbortTxn, txn_id, 0, 0)
    }

    pub fn new_v1(body: WalRecordBody, txn_id: u64, flags: u16, lsn: u64) -> Self {
        Self {
            header: WalRecordHeader::V1(WalHeaderV1 {
                rec_type: body.kind() as u8,
                rec_ver: WalHeaderVersion::V1,
                flags,
                lsn,
                txn_id,
            }),
            body,
        }
    }

    pub fn new_v2(
        body: WalRecordBody,
        txn_id: u64,
        flags: u16,
        lsn: u64,
        prev_hash: [u8; 32],
        rec_hash: [u8; 32],
    ) -> Self {
        Self {
            header: WalRecordHeader::V2(WalHeaderV2 {
                v1: WalHeaderV1 {
                    rec_type: body.kind() as u8,
                    rec_ver: WalHeaderVersion::V2,
                    flags,
                    lsn,
                    txn_id,
                },
                prev_hash,
                rec_hash,
            }),
            body,
        }
    }

    pub fn with_lsn(mut self, lsn: u64) -> Self {
        match &mut self.header {
            WalRecordHeader::V1(h) => h.lsn = lsn,
            WalRecordHeader::V2(h) => h.v1.lsn = lsn,
        }
        self
    }

    pub fn with_flags(mut self, flags: u16) -> Self {
        match &mut self.header {
            WalRecordHeader::V1(h) => h.flags = flags,
            WalRecordHeader::V2(h) => h.v1.flags = flags,
        }
        self
    }

    pub fn lsn(&self) -> u64 {
        match &self.header {
            WalRecordHeader::V1(h) => h.lsn,
            WalRecordHeader::V2(h) => h.v1.lsn,
        }
    }

    pub fn txn_id(&self) -> u64 {
        match &self.header {
            WalRecordHeader::V1(h) => h.txn_id,
            WalRecordHeader::V2(h) => h.v1.txn_id,
        }
    }

    pub fn flags(&self) -> u16 {
        match &self.header {
            WalRecordHeader::V1(h) => h.flags,
            WalRecordHeader::V2(h) => h.v1.flags,
        }
    }

    pub fn kind(&self) -> WalRecordKind {
        self.body.kind()
    }

    pub fn encode(&self) -> Result<Vec<u8>, WalError> {
        let mut out = Vec::new();
        match &self.header {
            WalRecordHeader::V1(h) => {
                out.push(self.body.kind() as u8);
                out.push(h.rec_ver as u8);
                out.extend_from_slice(&h.flags.to_le_bytes());
                out.extend_from_slice(&h.lsn.to_le_bytes());
                out.extend_from_slice(&h.txn_id.to_le_bytes());
            }
            WalRecordHeader::V2(h) => {
                out.push(self.body.kind() as u8);
                out.push(h.v1.rec_ver as u8);
                out.extend_from_slice(&h.v1.flags.to_le_bytes());
                out.extend_from_slice(&h.v1.lsn.to_le_bytes());
                out.extend_from_slice(&h.v1.txn_id.to_le_bytes());
                out.extend_from_slice(&h.prev_hash);
                out.extend_from_slice(&h.rec_hash);
            }
        }

        match &self.body {
            WalRecordBody::BeginTxn | WalRecordBody::CommitTxn | WalRecordBody::AbortTxn => {}
            WalRecordBody::Mutation { payload } => {
                out.extend_from_slice(&encode_varu(payload.len() as u64));
                out.extend_from_slice(payload);
            }
        }
        Ok(out)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, WalError> {
        if payload.len() < WAL_HEADER_V1_LEN {
            return Err(WalError::Format(FormatError::UnexpectedEof));
        }
        let rec_type = payload[0];
        let kind = WalRecordKind::from_u8(rec_type)?;
        let rec_ver = match payload[1] {
            1 => WalHeaderVersion::V1,
            2 => WalHeaderVersion::V2,
            other => return Err(WalError::UnsupportedRecordVersion(other)),
        };
        let flags = u16::from_le_bytes([payload[2], payload[3]]);
        let lsn = u64_from_le(payload, 4)?;
        let txn_id = u64_from_le(payload, 12)?;
        let (header, mut rest) = match rec_ver {
            WalHeaderVersion::V1 => (
                WalRecordHeader::V1(WalHeaderV1 {
                    rec_type,
                    rec_ver,
                    flags,
                    lsn,
                    txn_id,
                }),
                &payload[WAL_HEADER_V1_LEN..],
            ),
            WalHeaderVersion::V2 => {
                if payload.len() < WAL_HEADER_V2_LEN {
                    return Err(WalError::Format(FormatError::UnexpectedEof));
                }
                let mut prev_hash = [0u8; 32];
                prev_hash.copy_from_slice(&payload[20..52]);
                let mut rec_hash = [0u8; 32];
                rec_hash.copy_from_slice(&payload[52..84]);
                (
                    WalRecordHeader::V2(WalHeaderV2 {
                        v1: WalHeaderV1 {
                            rec_type,
                            rec_ver,
                            flags,
                            lsn,
                            txn_id,
                        },
                        prev_hash,
                        rec_hash,
                    }),
                    &payload[WAL_HEADER_V2_LEN..],
                )
            }
        };

        let body = match kind {
            WalRecordKind::BeginTxn => WalRecordBody::BeginTxn,
            WalRecordKind::CommitTxn => WalRecordBody::CommitTxn,
            WalRecordKind::AbortTxn => WalRecordBody::AbortTxn,
            WalRecordKind::Mutation => {
                let len = decode_varu(&mut rest).map_err(WalError::core)? as usize;
                if rest.len() < len {
                    return Err(WalError::Format(FormatError::UnexpectedEof));
                }
                let payload = rest[..len].to_vec();
                rest = &rest[len..];
                WalRecordBody::Mutation { payload }
            }
        };

        if !rest.is_empty() {
            return Err(WalError::TrailingBytes(rest.len()));
        }

        Ok(Self { header, body })
    }
}

#[derive(Debug, Error)]
pub enum WalError {
    #[error("wal file does not start with a valid FileHeader of kind Wal")]
    BadHeader,
    #[error("unknown WAL record type {0:#04x}")]
    UnknownRecordType(u8),
    #[error("unsupported WAL record version {0}")]
    UnsupportedRecordVersion(u8),
    #[error("WAL record has {0} trailing payload bytes")]
    TrailingBytes(usize),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl WalError {
    fn core(err: crate::CoreError) -> Self {
        match err {
            crate::CoreError::UnexpectedEof => WalError::Format(FormatError::UnexpectedEof),
            crate::CoreError::InvalidVarint => WalError::TrailingBytes(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalMutation {
    pub lsn: u64,
    pub flags: u16,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredTxn {
    pub txn_id: u64,
    pub begin_lsn: Option<u64>,
    pub commit_lsn: u64,
    pub mutations: Vec<WalMutation>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalRecovery {
    pub txns: Vec<RecoveredTxn>,
    pub last_valid_lsn: u64,
    pub truncated_tail: bool,
    pub discarded_tail_bytes: usize,
    pub tail_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct PendingTxn {
    begin_lsn: Option<u64>,
    mutations: Vec<WalMutation>,
}

#[derive(Debug, Clone, Default)]
struct ScanResult {
    records: Vec<WalRecord>,
    valid_bytes: usize,
    truncated_tail: bool,
    tail_error: Option<String>,
}

pub struct WalWriter {
    path: PathBuf,
    file: File,
    next_lsn: u64,
    truncated_tail: bool,
    discarded_tail_bytes: usize,
}

impl WalWriter {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let existed = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let len = file.metadata()?.len();

        if !existed || len == 0 {
            let header = FileHeader::new(FileKind::Wal, 0, now_unix_s());
            file.write_all(&header.encode())?;
            file.flush()?;
            file.seek(SeekFrom::End(0))?;
            return Ok(Self {
                path,
                file,
                next_lsn: 1,
                truncated_tail: false,
                discarded_tail_bytes: 0,
            });
        }

        if len < FILE_HEADER_LEN as u64 {
            return Err(WalError::BadHeader);
        }
        let scan = WalReader::open(&path)?.scan_lenient()?;
        let discarded_tail_bytes = if scan.truncated_tail {
            let discarded = (len as usize).saturating_sub(FILE_HEADER_LEN + scan.valid_bytes);
            file.set_len((FILE_HEADER_LEN + scan.valid_bytes) as u64)?;
            discarded
        } else {
            0
        };
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            path,
            file,
            next_lsn: scan.records.last().map(|r| r.lsn() + 1).unwrap_or(1),
            truncated_tail: scan.truncated_tail,
            discarded_tail_bytes,
        })
    }

    pub fn begin_txn(&mut self, txn_id: u64) -> Result<u64, WalError> {
        self.append(WalRecord::begin_v1(txn_id))
    }

    pub fn append_mutation(&mut self, txn_id: u64, payload: Vec<u8>) -> Result<u64, WalError> {
        self.append(WalRecord::mutation_v1(txn_id, payload))
    }

    pub fn commit_txn(&mut self, txn_id: u64) -> Result<u64, WalError> {
        self.append(WalRecord::commit_v1(txn_id))
    }

    pub fn abort_txn(&mut self, txn_id: u64) -> Result<u64, WalError> {
        self.append(WalRecord::abort_v1(txn_id))
    }

    pub fn append(&mut self, record: WalRecord) -> Result<u64, WalError> {
        let lsn = self.next_lsn;
        let record = record.with_lsn(lsn);
        let payload = record.encode()?;
        let mut frame = Vec::with_capacity(payload.len() + 8);
        append_record_frame(&mut frame, &payload)?;
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&frame)?;
        self.file.flush()?;
        self.next_lsn += 1;
        Ok(lsn)
    }

    pub fn sync(&self) -> Result<(), WalError> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    pub fn truncated_tail(&self) -> bool {
        self.truncated_tail
    }

    pub fn discarded_tail_bytes(&self) -> usize {
        self.discarded_tail_bytes
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct WalReader {
    path: PathBuf,
}

impl WalReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let mut header = [0u8; FILE_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|_| WalError::BadHeader)?;
        let header = FileHeader::decode(&header)?;
        if header.file_kind != FileKind::Wal {
            return Err(WalError::BadHeader);
        }
        Ok(Self { path })
    }

    pub fn read_all(&self) -> Result<Vec<WalRecord>, WalError> {
        Ok(self.scan_strict()?.records)
    }

    pub fn recover(&self) -> Result<WalRecovery, WalError> {
        let scan = self.scan_lenient()?;
        let discarded_tail_bytes = if scan.truncated_tail {
            self.tail_len()?.saturating_sub(scan.valid_bytes)
        } else {
            0
        };
        let mut pending: HashMap<u64, PendingTxn> = HashMap::new();
        let mut txns = Vec::new();
        let mut last_valid_lsn = 0;

        for rec in scan.records {
            let txn_id = rec.txn_id();
            let lsn = rec.lsn();
            let flags = rec.flags();
            last_valid_lsn = lsn;
            match rec.body {
                WalRecordBody::BeginTxn => {
                    pending.entry(txn_id).or_default().begin_lsn = Some(lsn);
                }
                WalRecordBody::Mutation { payload } => {
                    pending
                        .entry(txn_id)
                        .or_default()
                        .mutations
                        .push(WalMutation {
                            lsn,
                            flags,
                            payload,
                        });
                }
                WalRecordBody::CommitTxn => {
                    let staged = pending.remove(&txn_id).unwrap_or_default();
                    txns.push(RecoveredTxn {
                        txn_id,
                        begin_lsn: staged.begin_lsn,
                        commit_lsn: lsn,
                        mutations: staged.mutations,
                    });
                }
                WalRecordBody::AbortTxn => {
                    pending.remove(&txn_id);
                }
            }
        }

        Ok(WalRecovery {
            txns,
            last_valid_lsn,
            truncated_tail: scan.truncated_tail,
            discarded_tail_bytes,
            tail_error: scan.tail_error,
        })
    }

    fn scan_strict(&self) -> Result<ScanResult, WalError> {
        self.scan(true)
    }

    fn scan_lenient(&self) -> Result<ScanResult, WalError> {
        self.scan(false)
    }

    fn scan(&self, strict: bool) -> Result<ScanResult, WalError> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        file.seek(SeekFrom::Start(FILE_HEADER_LEN as u64))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        let mut rest: &[u8] = &bytes;
        let mut records = Vec::new();
        let mut valid_bytes = 0usize;

        while !rest.is_empty() {
            let frame_start = rest;
            let mut after_frame = frame_start;
            let payload = match decode_record_frame(&mut after_frame) {
                Ok(payload) => payload,
                Err(err) => {
                    if strict {
                        return Err(WalError::Format(err));
                    }
                    return Ok(ScanResult {
                        records,
                        valid_bytes,
                        truncated_tail: true,
                        tail_error: Some(err.to_string()),
                    });
                }
            };
            let rec = match WalRecord::decode(&payload) {
                Ok(rec) => rec,
                Err(err) => {
                    if strict {
                        return Err(err);
                    }
                    return Ok(ScanResult {
                        records,
                        valid_bytes,
                        truncated_tail: true,
                        tail_error: Some(err.to_string()),
                    });
                }
            };
            valid_bytes += frame_start.len() - after_frame.len();
            records.push(rec);
            rest = after_frame;
        }

        Ok(ScanResult {
            records,
            valid_bytes,
            truncated_tail: false,
            tail_error: None,
        })
    }

    fn tail_len(&self) -> Result<usize, WalError> {
        let len = std::fs::metadata(&self.path)?.len() as usize;
        Ok(len.saturating_sub(FILE_HEADER_LEN))
    }
}

fn u64_from_le(bytes: &[u8], offset: usize) -> Result<u64, WalError> {
    if bytes.len().saturating_sub(offset) < 8 {
        return Err(WalError::Format(FormatError::UnexpectedEof));
    }
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    Ok(u64::from_le_bytes(out))
}

fn now_unix_s() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(label: &str) -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut p = std::env::temp_dir();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!(
            "skeindb_core_wal_{}_{}_{}",
            label,
            std::process::id(),
            ts
        ));
        p
    }

    #[test]
    fn wal_record_roundtrip_v1_variants() -> Result<(), WalError> {
        let records = vec![
            WalRecord::begin_v1(7).with_lsn(10),
            WalRecord::mutation_v1(7, b"hello".to_vec())
                .with_flags(3)
                .with_lsn(11),
            WalRecord::commit_v1(7).with_lsn(12),
            WalRecord::abort_v1(8).with_lsn(13),
        ];
        for record in records {
            let decoded = WalRecord::decode(&record.encode()?)?;
            assert_eq!(decoded, record);
        }
        Ok(())
    }

    #[test]
    fn wal_record_roundtrip_v2_mutation() -> Result<(), WalError> {
        let decoded = WalRecord::decode(
            &WalRecord::new_v2(
                WalRecordBody::Mutation {
                    payload: b"payload".to_vec(),
                },
                9,
                5,
                42,
                [1u8; 32],
                [2u8; 32],
            )
            .encode()?,
        )?;
        assert_eq!(decoded.lsn(), 42);
        assert_eq!(decoded.txn_id(), 9);
        assert_eq!(decoded.flags(), 5);
        assert_eq!(decoded.kind(), WalRecordKind::Mutation);
        Ok(())
    }

    #[test]
    fn wal_decode_rejects_unknown_record_type() {
        let err = WalRecord::decode(&[
            0x7f, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ])
        .unwrap_err();
        match err {
            WalError::UnknownRecordType(0x7f) => {}
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn wal_recovery_only_replays_committed_txns() -> Result<(), WalError> {
        let path = temp_path("recovery_committed");
        let mut writer = WalWriter::open(&path)?;
        writer.begin_txn(1)?;
        writer.append_mutation(1, b"a1".to_vec())?;
        writer.begin_txn(2)?;
        writer.append_mutation(2, b"b1".to_vec())?;
        writer.commit_txn(1)?;
        writer.abort_txn(2)?;
        writer.sync()?;

        let recovery = WalReader::open(&path)?.recover()?;
        assert_eq!(recovery.txns.len(), 1);
        assert_eq!(recovery.txns[0].txn_id, 1);
        assert_eq!(recovery.txns[0].mutations.len(), 1);
        assert_eq!(recovery.txns[0].mutations[0].payload, b"a1");

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn wal_writer_reopen_truncates_torn_tail() -> Result<(), WalError> {
        let path = temp_path("reopen_truncate");
        {
            let mut writer = WalWriter::open(&path)?;
            writer.begin_txn(1)?;
            writer.append_mutation(1, b"abc".to_vec())?;
            writer.commit_txn(1)?;
            writer.sync()?;
        }
        let before = std::fs::metadata(&path)?.len();
        {
            let mut file = OpenOptions::new().append(true).open(&path)?;
            file.write_all(&[0xde, 0xad, 0xbe])?;
            file.flush()?;
        }
        let after_corrupt = std::fs::metadata(&path)?.len();
        assert!(after_corrupt > before);

        let writer = WalWriter::open(&path)?;
        assert!(writer.truncated_tail());
        assert_eq!(writer.discarded_tail_bytes(), 3);
        assert_eq!(writer.next_lsn(), 4);
        assert_eq!(std::fs::metadata(&path)?.len(), before);

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn wal_reader_rejects_non_wal_header() -> Result<(), WalError> {
        let path = temp_path("bad_header");
        std::fs::write(
            &path,
            FileHeader::new(FileKind::Manifest, 0, now_unix_s()).encode(),
        )?;
        let err = match WalReader::open(&path) {
            Ok(_) => panic!("expected non-wal header to fail"),
            Err(err) => err,
        };
        match err {
            WalError::BadHeader => {}
            other => panic!("unexpected error: {other}"),
        }
        let _ = std::fs::remove_file(path);
        Ok(())
    }
}
