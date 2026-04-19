//! Row segments (`.rseg`) + RowVersion encoding (T014).
//!
//! Implements the RV1 record format described in `docs/ON_DISK_FORMAT.md`
//! Section 6. Row segments are append-only containers of MVCC row versions.
//! Each appended [`RowVersion`] returns a [`FilePtr`] that callers can chain
//! via [`RowVersion::prev_ptr`] to form per-row version chains.
//!
//! Scope for this slice:
//!
//! - `FilePtr` (12 bytes: `file_id: u32`, `offset: u64`).
//! - `RowGroupRef` (`Inline` bytes or `ValueId` reference).
//! - `RowVersion` encode/decode for `rec_type=0x10 rec_ver=1` records.
//! - `RowSegmentWriter::create/open/append/sync` over
//!   `FileHeader(FileKind::RowSeg)` + `RecordFrame` framing.
//! - `RowSegmentReader::open/read_all/read_at` with strict validation.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{
    append_record_frame, decode_record_frame, decode_varu, encode_varu, CoreError, FileHeader,
    FileKind, FormatError, FILE_HEADER_LEN,
};

/// Row segment record tag (`0x10`) per `docs/ON_DISK_FORMAT.md` §6.
pub const RSEG_RECORD_TYPE: u8 = 0x10;
/// Supported record version.
pub const RSEG_RECORD_VERSION: u8 = 1;

/// `IS_DELETE` flag bit on [`RowVersion::flags`].
pub const ROW_FLAG_IS_DELETE: u16 = 0x0001;

const GROUP_REF_INLINE: u8 = 0;
const GROUP_REF_VALUE_ID: u8 = 1;

const FILE_PTR_LEN: usize = 12;
/// Byte size of a serialized [`FilePtr`] (`file_id u32 LE` + `offset u64 LE`).
pub const FILE_PTR_ENCODED_LEN: usize = FILE_PTR_LEN;

/// Length of a BLAKE3-128 ValueID referenced from a row group.
pub const VALUE_ID_LEN: usize = 16;

#[derive(Debug, Error)]
pub enum RowSegError {
    #[error("row segment file does not start with a valid FileHeader of kind RowSeg")]
    BadHeader,
    #[error("invalid row segment record type {0:#04x}")]
    InvalidRecordType(u8),
    #[error("unsupported row segment record version {0}")]
    UnsupportedRecordVersion(u8),
    #[error("invalid row group ref kind {0}")]
    InvalidGroupRefKind(u8),
    #[error("invalid row segment record: {0}")]
    InvalidRecord(&'static str),
    #[error("trailing bytes after row segment record payload")]
    TrailingRecordBytes,
    #[error("row segment writer is already closed")]
    WriterClosed,
    #[error("value id length mismatch (expected 16 bytes)")]
    InvalidValueIdLength,
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Core(#[from] CoreError),
}

/// Pointer to a record inside a segment file.
///
/// `FilePtr::default()` (`file_id=0, offset=0`) is used as the sentinel "no
/// previous version" marker in MVCC chains.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FilePtr {
    pub file_id: u32,
    pub offset: u64,
}

impl FilePtr {
    pub const NONE: FilePtr = FilePtr {
        file_id: 0,
        offset: 0,
    };

    pub fn new(file_id: u32, offset: u64) -> Self {
        Self { file_id, offset }
    }

    pub fn is_none(&self) -> bool {
        self.file_id == 0 && self.offset == 0
    }

    pub fn encode(&self) -> [u8; FILE_PTR_LEN] {
        let mut out = [0u8; FILE_PTR_LEN];
        out[0..4].copy_from_slice(&self.file_id.to_le_bytes());
        out[4..12].copy_from_slice(&self.offset.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RowSegError> {
        if bytes.len() < FILE_PTR_LEN {
            return Err(RowSegError::InvalidRecord("short FilePtr"));
        }
        let file_id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        Ok(Self { file_id, offset })
    }
}

/// How a row group's payload is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowGroupRef {
    /// Inline GroupObject bytes (`group_ref_kind=0`).
    Inline(Vec<u8>),
    /// Content-addressed reference to a GROUP in the ValueStore
    /// (`group_ref_kind=1`, 16-byte ValueID).
    ValueId([u8; VALUE_ID_LEN]),
}

/// One group inside a row version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowGroup {
    pub group_id: u64,
    pub data: RowGroupRef,
}

/// An MVCC row version record (RV1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowVersion {
    pub flags: u16,
    pub table_id: u32,
    pub row_id: u64,
    /// Commit timestamp. `0` is only valid for WAL-staged rows.
    pub begin_ts: u64,
    /// `0` means `+INF` (this is the current live version).
    pub end_ts: u64,
    /// Previous version pointer; use [`FilePtr::NONE`] for the head of a chain.
    pub prev_ptr: FilePtr,
    pub groups: Vec<RowGroup>,
}

impl RowVersion {
    /// Build a non-deleted row version with no previous pointer.
    pub fn new(table_id: u32, row_id: u64, begin_ts: u64, groups: Vec<RowGroup>) -> Self {
        Self {
            flags: 0,
            table_id,
            row_id,
            begin_ts,
            end_ts: 0,
            prev_ptr: FilePtr::NONE,
            groups,
        }
    }

    pub fn with_prev(mut self, prev: FilePtr) -> Self {
        self.prev_ptr = prev;
        self
    }

    pub fn with_end_ts(mut self, end_ts: u64) -> Self {
        self.end_ts = end_ts;
        self
    }

    pub fn with_is_delete(mut self, is_delete: bool) -> Self {
        if is_delete {
            self.flags |= ROW_FLAG_IS_DELETE;
        } else {
            self.flags &= !ROW_FLAG_IS_DELETE;
        }
        self
    }

    pub fn is_delete(&self) -> bool {
        self.flags & ROW_FLAG_IS_DELETE != 0
    }

    /// Encode the payload of one RV1 record (without the enclosing
    /// `RecordFrame`).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.push(RSEG_RECORD_TYPE);
        out.push(RSEG_RECORD_VERSION);
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.table_id.to_le_bytes());
        out.extend_from_slice(&self.row_id.to_le_bytes());
        out.extend_from_slice(&self.begin_ts.to_le_bytes());
        out.extend_from_slice(&self.end_ts.to_le_bytes());
        out.extend_from_slice(&self.prev_ptr.encode());
        out.extend_from_slice(&encode_varu(self.groups.len() as u64));
        for group in &self.groups {
            out.extend_from_slice(&encode_varu(group.group_id));
            match &group.data {
                RowGroupRef::Inline(bytes) => {
                    out.push(GROUP_REF_INLINE);
                    out.extend_from_slice(&encode_varu(bytes.len() as u64));
                    out.extend_from_slice(bytes);
                }
                RowGroupRef::ValueId(id) => {
                    out.push(GROUP_REF_VALUE_ID);
                    out.extend_from_slice(id);
                }
            }
        }
        out
    }

    /// Decode one RV1 record payload previously produced by [`RowVersion::encode`].
    pub fn decode(payload: &[u8]) -> Result<Self, RowSegError> {
        let mut cursor = payload;
        if cursor.len() < 2 {
            return Err(RowSegError::InvalidRecord("short RV1 header"));
        }
        let rec_type = cursor[0];
        let rec_ver = cursor[1];
        cursor = &cursor[2..];
        if rec_type != RSEG_RECORD_TYPE {
            return Err(RowSegError::InvalidRecordType(rec_type));
        }
        if rec_ver != RSEG_RECORD_VERSION {
            return Err(RowSegError::UnsupportedRecordVersion(rec_ver));
        }

        let flags = take_u16(&mut cursor, "flags")?;
        let table_id = take_u32(&mut cursor, "table_id")?;
        let row_id = take_u64(&mut cursor, "row_id")?;
        let begin_ts = take_u64(&mut cursor, "begin_ts")?;
        let end_ts = take_u64(&mut cursor, "end_ts")?;

        if cursor.len() < FILE_PTR_LEN {
            return Err(RowSegError::InvalidRecord("short prev_ptr"));
        }
        let prev_ptr = FilePtr::decode(&cursor[..FILE_PTR_LEN])?;
        cursor = &cursor[FILE_PTR_LEN..];

        let group_count = decode_varu(&mut cursor)?;
        if group_count > u32::MAX as u64 {
            return Err(RowSegError::InvalidRecord("group_count too large"));
        }
        let mut groups = Vec::with_capacity(group_count as usize);
        for _ in 0..group_count {
            let group_id = decode_varu(&mut cursor)?;
            if cursor.is_empty() {
                return Err(RowSegError::InvalidRecord("missing group_ref_kind"));
            }
            let kind = cursor[0];
            cursor = &cursor[1..];
            let data = match kind {
                GROUP_REF_INLINE => {
                    let len = decode_varu(&mut cursor)? as usize;
                    if cursor.len() < len {
                        return Err(RowSegError::InvalidRecord("short inline group bytes"));
                    }
                    let bytes = cursor[..len].to_vec();
                    cursor = &cursor[len..];
                    RowGroupRef::Inline(bytes)
                }
                GROUP_REF_VALUE_ID => {
                    if cursor.len() < VALUE_ID_LEN {
                        return Err(RowSegError::InvalidRecord("short value_id"));
                    }
                    let mut id = [0u8; VALUE_ID_LEN];
                    id.copy_from_slice(&cursor[..VALUE_ID_LEN]);
                    cursor = &cursor[VALUE_ID_LEN..];
                    RowGroupRef::ValueId(id)
                }
                other => return Err(RowSegError::InvalidGroupRefKind(other)),
            };
            groups.push(RowGroup { group_id, data });
        }

        if !cursor.is_empty() {
            return Err(RowSegError::TrailingRecordBytes);
        }

        Ok(Self {
            flags,
            table_id,
            row_id,
            begin_ts,
            end_ts,
            prev_ptr,
            groups,
        })
    }
}

fn take_u16(cursor: &mut &[u8], what: &'static str) -> Result<u16, RowSegError> {
    if cursor.len() < 2 {
        return Err(RowSegError::InvalidRecord(what));
    }
    let v = u16::from_le_bytes(cursor[..2].try_into().unwrap());
    *cursor = &cursor[2..];
    Ok(v)
}

fn take_u32(cursor: &mut &[u8], what: &'static str) -> Result<u32, RowSegError> {
    if cursor.len() < 4 {
        return Err(RowSegError::InvalidRecord(what));
    }
    let v = u32::from_le_bytes(cursor[..4].try_into().unwrap());
    *cursor = &cursor[4..];
    Ok(v)
}

fn take_u64(cursor: &mut &[u8], what: &'static str) -> Result<u64, RowSegError> {
    if cursor.len() < 8 {
        return Err(RowSegError::InvalidRecord(what));
    }
    let v = u64::from_le_bytes(cursor[..8].try_into().unwrap());
    *cursor = &cursor[8..];
    Ok(v)
}

/// One decoded row version + the file offset where its `RecordFrame` starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowSegmentEntry {
    /// File offset of the start of the `RecordFrame` that carried this record.
    /// Use `FilePtr { file_id, offset }` to chain previous versions.
    pub offset: u64,
    pub row: RowVersion,
}

/// Append-only writer for `.rseg` files.
pub struct RowSegmentWriter {
    file: File,
    path: PathBuf,
    file_id: u32,
    /// Byte offset where the *next* `RecordFrame` will start.
    cursor_offset: u64,
    closed: bool,
}

impl RowSegmentWriter {
    /// Create a new `.rseg` file. Fails if the file already exists.
    pub fn create<P: AsRef<Path>>(
        path: P,
        file_id: u32,
        created_unix_s: u64,
    ) -> Result<Self, RowSegError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let header = FileHeader::new(FileKind::RowSeg, file_id, created_unix_s).encode();
        file.write_all(&header)?;
        file.flush()?;
        Ok(Self {
            file,
            path,
            file_id,
            cursor_offset: FILE_HEADER_LEN as u64,
            closed: false,
        })
    }

    /// Open an existing `.rseg` file for appending. Validates header, seeks to end.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, RowSegError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let mut header_bytes = [0u8; FILE_HEADER_LEN];
        file.read_exact(&mut header_bytes)?;
        let header = FileHeader::decode(&header_bytes)?;
        if header.file_kind != FileKind::RowSeg {
            return Err(RowSegError::BadHeader);
        }
        let end = file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            path,
            file_id: header.file_id,
            cursor_offset: end,
            closed: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_id(&self) -> u32 {
        self.file_id
    }

    /// Append a row version and return a [`FilePtr`] pointing at the start of
    /// the `RecordFrame` that carries it.
    pub fn append(&mut self, row: &RowVersion) -> Result<FilePtr, RowSegError> {
        if self.closed {
            return Err(RowSegError::WriterClosed);
        }
        let payload = row.encode();
        let mut buf: Vec<u8> = Vec::with_capacity(payload.len() + 8);
        append_record_frame(&mut buf, &payload)?;
        let record_offset = self.cursor_offset;
        self.file.write_all(&buf)?;
        self.cursor_offset = record_offset
            .checked_add(buf.len() as u64)
            .ok_or(RowSegError::InvalidRecord("file offset overflow"))?;
        Ok(FilePtr::new(self.file_id, record_offset))
    }

    pub fn sync(&mut self) -> Result<(), RowSegError> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

/// Reader over a complete `.rseg` file.
pub struct RowSegmentReader {
    file: File,
    file_id: u32,
    end: u64,
}

impl RowSegmentReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, RowSegError> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut header_bytes = [0u8; FILE_HEADER_LEN];
        file.read_exact(&mut header_bytes)?;
        let header = FileHeader::decode(&header_bytes)?;
        if header.file_kind != FileKind::RowSeg {
            return Err(RowSegError::BadHeader);
        }
        let end = file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            file_id: header.file_id,
            end,
        })
    }

    pub fn file_id(&self) -> u32 {
        self.file_id
    }

    /// Read all row versions sequentially.
    pub fn read_all(&mut self) -> Result<Vec<RowSegmentEntry>, RowSegError> {
        let mut out = Vec::new();
        let mut offset = FILE_HEADER_LEN as u64;
        while offset < self.end {
            let (entry, next) = self.read_frame_at(offset)?;
            out.push(entry);
            offset = next;
        }
        Ok(out)
    }

    /// Read a single row version starting at `offset` (as returned by
    /// [`RowSegmentWriter::append`]).
    pub fn read_at(&mut self, offset: u64) -> Result<RowSegmentEntry, RowSegError> {
        if offset < FILE_HEADER_LEN as u64 || offset >= self.end {
            return Err(RowSegError::InvalidRecord("offset outside file"));
        }
        let (entry, _) = self.read_frame_at(offset)?;
        Ok(entry)
    }

    fn read_frame_at(&mut self, offset: u64) -> Result<(RowSegmentEntry, u64), RowSegError> {
        let remaining = self.end.saturating_sub(offset) as usize;
        if remaining < 8 {
            return Err(RowSegError::InvalidRecord("short record frame header"));
        }
        let to_read = remaining.min(16 * 1024 * 1024);
        let mut buf = vec![0u8; to_read];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        let mut slice: &[u8] = &buf;
        let before = slice.len();
        let payload = decode_record_frame(&mut slice)?;
        let consumed = before - slice.len();
        let row = RowVersion::decode(&payload)?;
        Ok((RowSegmentEntry { offset, row }, offset + consumed as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row() -> RowVersion {
        RowVersion::new(
            7,
            42,
            1_700_000_000,
            vec![
                RowGroup {
                    group_id: 0,
                    data: RowGroupRef::Inline(b"{\"a\":1}".to_vec()),
                },
                RowGroup {
                    group_id: 1,
                    data: RowGroupRef::ValueId([9u8; VALUE_ID_LEN]),
                },
            ],
        )
        .with_prev(FilePtr::new(3, 1024))
    }

    #[test]
    fn row_version_roundtrip() {
        let row = sample_row();
        let bytes = row.encode();
        let decoded = RowVersion::decode(&bytes).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn row_version_rejects_unknown_rec_type() {
        let mut bytes = sample_row().encode();
        bytes[0] = 0x99;
        let err = RowVersion::decode(&bytes).expect_err("bad type");
        assert!(matches!(err, RowSegError::InvalidRecordType(0x99)));
    }

    #[test]
    fn row_version_rejects_unknown_rec_ver() {
        let mut bytes = sample_row().encode();
        bytes[1] = 2;
        let err = RowVersion::decode(&bytes).expect_err("bad ver");
        assert!(matches!(err, RowSegError::UnsupportedRecordVersion(2)));
    }

    #[test]
    fn row_version_rejects_trailing_bytes() {
        let mut bytes = sample_row().encode();
        bytes.push(0);
        let err = RowVersion::decode(&bytes).expect_err("trailing");
        assert!(matches!(err, RowSegError::TrailingRecordBytes));
    }

    #[test]
    fn file_ptr_roundtrip_and_sentinel() {
        let p = FilePtr::new(12, 0x1_0000_0000);
        assert!(!p.is_none());
        assert_eq!(FilePtr::decode(&p.encode()).unwrap(), p);
        assert!(FilePtr::NONE.is_none());
        assert_eq!(
            FilePtr::decode(&FilePtr::NONE.encode()).unwrap(),
            FilePtr::NONE
        );
    }

    #[test]
    fn delete_flag_roundtrip() {
        let row = sample_row().with_is_delete(true);
        assert!(row.is_delete());
        let decoded = RowVersion::decode(&row.encode()).unwrap();
        assert!(decoded.is_delete());
    }
}
