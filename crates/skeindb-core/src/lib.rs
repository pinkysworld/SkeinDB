//! SkeinDB Core (scaffold)
//!
//! This crate is intended to host:
//! - on-disk formats (WAL, segments)
//! - MVCC engine
//! - deduplicated ValueStore (content-addressed)
//! - compaction + GC
//!
//! For now it contains:
//! - basic encoding helpers (VarU, CRC32C)
//! - small format enums/structs used across the project

use thiserror::Error;

pub mod manifest;
pub mod rowdir;
pub mod rowseg;
pub mod run;
pub mod valuestore;
pub mod wal;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("unexpected EOF")]
    UnexpectedEof,

    #[error("invalid varint")]
    InvalidVarint,
}

/// Encode a u64 as unsigned LEB128 (VarU).
pub fn encode_varu(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
    out
}

/// Decode a u64 from unsigned LEB128 (VarU).
pub fn decode_varu(input: &mut &[u8]) -> Result<u64, CoreError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    for _ in 0..10 {
        let Some((&b, rest)) = input.split_first() else {
            return Err(CoreError::UnexpectedEof);
        };
        *input = rest;

        let payload = (b & 0x7F) as u64;
        result |= payload << shift;

        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }

    Err(CoreError::InvalidVarint)
}

/// CRC32C of a byte slice.
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Compute ValueID = BLAKE3-128 (first 16 bytes).
pub fn value_id(bytes: &[u8]) -> [u8; 16] {
    let h = blake3::hash(bytes);
    let mut out = [0u8; 16];
    out.copy_from_slice(&h.as_bytes()[0..16]);
    out
}

/// Compute an audit hash = BLAKE3-256.
pub fn audit_hash256(bytes: &[u8]) -> [u8; 32] {
    let h = blake3::hash(bytes);
    *h.as_bytes()
}

/// Value kinds for ValueStore entries.
///
/// These numeric values are part of the on-disk format contract (docs/ON_DISK_FORMAT.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueKind {
    /// Raw value bytes.
    Cell = 1,
    /// Group object encoding (e.g., small tuple/struct).
    Group = 2,
    /// Chunk of a large blob.
    BlobChunk = 3,
    /// Manifest referencing blob chunks.
    BlobManifest = 4,
    /// Delta patch against a base ValueID.
    Delta = 5,
    /// Embedding vector (research / experimental).
    Embedding = 6,
}

/// WAL record header versions for audit chaining.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalHeaderVersion {
    V1 = 1,
    V2 = 2,
}

/// Minimal WAL header fields (v1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalHeaderV1 {
    pub rec_type: u8,
    pub rec_ver: WalHeaderVersion,
    pub flags: u16,
    pub lsn: u64,
    pub txn_id: u64,
}

/// WAL header v2 adds hash chaining fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalHeaderV2 {
    pub v1: WalHeaderV1,
    pub prev_hash: [u8; 32],
    pub rec_hash: [u8; 32],
}

/// Compatibility telemetry feature identifiers (seed list).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CompatFeature {
    MysqlSqlCalcFoundRows,
    MysqlFoundRows,
    MysqlInsertOnDuplicateKeyUpdate,
    MysqlInformationSchema,
    MysqlShow,
}

pub const FILE_HEADER_LEN: usize = 64;
pub const FILE_HEADER_MAGIC: [u8; 8] = *b"SKNDB\0\x01\0";
pub const FILE_HEADER_ENDIAN_LE: u8 = 1;
pub const FILE_HEADER_FORMAT_VERSION: u32 = 1;
pub const RECORD_FRAME_HEADER_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FileKind {
    Wal = 1,
    RowSeg = 2,
    ValSeg = 3,
    Run = 4,
    Manifest = 5,
}

impl TryFrom<u8> for FileKind {
    type Error = FormatError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Wal),
            2 => Ok(Self::RowSeg),
            3 => Ok(Self::ValSeg),
            4 => Ok(Self::Run),
            5 => Ok(Self::Manifest),
            _ => Err(FormatError::UnknownFileKind(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub file_kind: FileKind,
    pub format_ver: u32,
    pub file_id: u32,
    pub created_unix_s: u64,
    pub reserved: [u8; 32],
}

impl FileHeader {
    pub fn new(file_kind: FileKind, file_id: u32, created_unix_s: u64) -> Self {
        Self {
            file_kind,
            format_ver: FILE_HEADER_FORMAT_VERSION,
            file_id,
            created_unix_s,
            reserved: [0u8; 32],
        }
    }

    pub fn encode(&self) -> [u8; FILE_HEADER_LEN] {
        let mut out = [0u8; FILE_HEADER_LEN];
        out[0..8].copy_from_slice(&FILE_HEADER_MAGIC);
        out[8] = self.file_kind as u8;
        out[9] = FILE_HEADER_ENDIAN_LE;
        out[10..12].copy_from_slice(&(FILE_HEADER_LEN as u16).to_le_bytes());
        out[12..16].copy_from_slice(&self.format_ver.to_le_bytes());
        out[16..20].copy_from_slice(&self.file_id.to_le_bytes());
        out[20..28].copy_from_slice(&self.created_unix_s.to_le_bytes());
        out[28..60].copy_from_slice(&self.reserved);
        let crc = crc32c(&out[0..60]);
        out[60..64].copy_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < FILE_HEADER_LEN {
            return Err(FormatError::UnexpectedEof);
        }
        let header = &bytes[0..FILE_HEADER_LEN];
        if header[0..8] != FILE_HEADER_MAGIC {
            return Err(FormatError::InvalidFileHeaderMagic);
        }

        let file_kind = FileKind::try_from(header[8])?;
        let endian = header[9];
        if endian != FILE_HEADER_ENDIAN_LE {
            return Err(FormatError::UnsupportedEndian(endian));
        }

        let header_len = u16_le(header, 10)?;
        if header_len as usize != FILE_HEADER_LEN {
            return Err(FormatError::InvalidFileHeaderLength(header_len));
        }

        let format_ver = u32_le(header, 12)?;
        if format_ver != FILE_HEADER_FORMAT_VERSION {
            return Err(FormatError::UnsupportedFileFormatVersion(format_ver));
        }

        let expected_crc = u32_le(header, 60)?;
        let actual_crc = crc32c(&header[0..60]);
        if expected_crc != actual_crc {
            return Err(FormatError::FileHeaderCrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        let mut reserved = [0u8; 32];
        reserved.copy_from_slice(&header[28..60]);

        Ok(Self {
            file_kind,
            format_ver,
            file_id: u32_le(header, 16)?,
            created_unix_s: u64_le(header, 20)?,
            reserved,
        })
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FormatError {
    #[error("unexpected EOF")]
    UnexpectedEof,

    #[error("invalid file header magic")]
    InvalidFileHeaderMagic,

    #[error("invalid file header length: {0}")]
    InvalidFileHeaderLength(u16),

    #[error("unsupported file format version: {0}")]
    UnsupportedFileFormatVersion(u32),

    #[error("unsupported endian flag: {0}")]
    UnsupportedEndian(u8),

    #[error("unknown file kind: {0}")]
    UnknownFileKind(u8),

    #[error("file header CRC mismatch (expected {expected:#010x}, got {actual:#010x})")]
    FileHeaderCrcMismatch { expected: u32, actual: u32 },

    #[error("record frame payload too large: {0}")]
    RecordFrameTooLarge(usize),

    #[error("record frame CRC mismatch (expected {expected:#010x}, got {actual:#010x})")]
    RecordFrameCrcMismatch { expected: u32, actual: u32 },
}

pub fn append_record_frame(buf: &mut Vec<u8>, payload: &[u8]) -> Result<(), FormatError> {
    let len_u32 = u32::try_from(payload.len())
        .map_err(|_| FormatError::RecordFrameTooLarge(payload.len()))?;
    buf.extend_from_slice(&len_u32.to_le_bytes());
    buf.extend_from_slice(&crc32c(payload).to_le_bytes());
    buf.extend_from_slice(payload);
    Ok(())
}

pub fn decode_record_frame(input: &mut &[u8]) -> Result<Vec<u8>, FormatError> {
    if input.len() < RECORD_FRAME_HEADER_LEN {
        return Err(FormatError::UnexpectedEof);
    }
    let len = u32_le(input, 0)? as usize;
    let expected_crc = u32_le(input, 4)?;
    if input.len() < RECORD_FRAME_HEADER_LEN + len {
        return Err(FormatError::UnexpectedEof);
    }
    let payload = &input[RECORD_FRAME_HEADER_LEN..RECORD_FRAME_HEADER_LEN + len];
    let actual_crc = crc32c(payload);
    if expected_crc != actual_crc {
        return Err(FormatError::RecordFrameCrcMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    *input = &input[RECORD_FRAME_HEADER_LEN + len..];
    Ok(payload.to_vec())
}

pub struct RecordFrameIter<'a> {
    rest: &'a [u8],
    done: bool,
}

impl<'a> Iterator for RecordFrameIter<'a> {
    type Item = Result<Vec<u8>, FormatError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.rest.is_empty() {
            return None;
        }
        let mut slice = self.rest;
        match decode_record_frame(&mut slice) {
            Ok(payload) => {
                self.rest = slice;
                Some(Ok(payload))
            }
            Err(err) => {
                self.done = true;
                Some(Err(err))
            }
        }
    }
}

pub fn iter_record_frames(bytes: &[u8]) -> RecordFrameIter<'_> {
    RecordFrameIter {
        rest: bytes,
        done: false,
    }
}

fn u16_le(bytes: &[u8], offset: usize) -> Result<u16, FormatError> {
    if bytes.len().saturating_sub(offset) < 2 {
        return Err(FormatError::UnexpectedEof);
    }
    let mut out = [0u8; 2];
    out.copy_from_slice(&bytes[offset..offset + 2]);
    Ok(u16::from_le_bytes(out))
}

fn u32_le(bytes: &[u8], offset: usize) -> Result<u32, FormatError> {
    if bytes.len().saturating_sub(offset) < 4 {
        return Err(FormatError::UnexpectedEof);
    }
    let mut out = [0u8; 4];
    out.copy_from_slice(&bytes[offset..offset + 4]);
    Ok(u32::from_le_bytes(out))
}

fn u64_le(bytes: &[u8], offset: usize) -> Result<u64, FormatError> {
    if bytes.len().saturating_sub(offset) < 8 {
        return Err(FormatError::UnexpectedEof);
    }
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[offset..offset + 8]);
    Ok(u64::from_le_bytes(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varu_roundtrip_small() {
        for v in 0u64..10000 {
            let enc = encode_varu(v);
            let mut slice: &[u8] = &enc;
            let dec = decode_varu(&mut slice).unwrap();
            assert_eq!(v, dec);
            assert!(slice.is_empty());
        }
    }

    #[test]
    fn varu_roundtrip_large() {
        let values = [0u64, 1, 127, 128, 255, 16384, u32::MAX as u64, u64::MAX];
        for v in values {
            let enc = encode_varu(v);
            let mut slice: &[u8] = &enc;
            let dec = decode_varu(&mut slice).unwrap();
            assert_eq!(v, dec);
        }
    }

    #[test]
    fn value_id_is_stable() {
        let a = value_id(b"hello");
        let b = value_id(b"hello");
        let c = value_id(b"world");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn audit_hash_is_stable() {
        let a = audit_hash256(b"hello");
        let b = audit_hash256(b"hello");
        assert_eq!(a, b);
    }

    #[test]
    fn file_header_roundtrip() {
        let mut header = FileHeader::new(FileKind::RowSeg, 42, 1_735_689_999);
        header.reserved[0] = 7;
        let bytes = header.encode();
        assert_eq!(bytes.len(), FILE_HEADER_LEN);
        let decoded = FileHeader::decode(&bytes).expect("decode file header");
        assert_eq!(decoded, header);
    }

    #[test]
    fn file_header_rejects_crc_mismatch() {
        let header = FileHeader::new(FileKind::Manifest, 9, 1234);
        let mut bytes = header.encode();
        bytes[24] ^= 0x01;
        let err = FileHeader::decode(&bytes).expect_err("crc mismatch expected");
        assert!(matches!(err, FormatError::FileHeaderCrcMismatch { .. }));
    }

    #[test]
    fn record_frame_append_and_iterate_roundtrip() {
        let mut file = Vec::new();
        append_record_frame(&mut file, b"alpha").expect("append alpha");
        append_record_frame(&mut file, b"beta").expect("append beta");
        append_record_frame(&mut file, b"").expect("append empty");

        let frames = iter_record_frames(&file)
            .map(|item| item.expect("frame decode"))
            .collect::<Vec<_>>();
        assert_eq!(
            frames,
            vec![b"alpha".to_vec(), b"beta".to_vec(), Vec::new()]
        );
    }

    #[test]
    fn record_frame_decode_detects_crc_mismatch() {
        let mut file = Vec::new();
        append_record_frame(&mut file, b"payload").expect("append payload");
        file[6] ^= 0x80;
        let mut slice: &[u8] = &file;
        let err = decode_record_frame(&mut slice).expect_err("crc mismatch expected");
        assert!(matches!(err, FormatError::RecordFrameCrcMismatch { .. }));
    }

    #[test]
    fn record_frame_decode_detects_truncation() {
        let mut file = Vec::new();
        append_record_frame(&mut file, b"payload").expect("append payload");
        file.truncate(file.len().saturating_sub(2));
        let mut slice: &[u8] = &file;
        let err = decode_record_frame(&mut slice).expect_err("truncation expected");
        assert_eq!(err, FormatError::UnexpectedEof);
    }
}
