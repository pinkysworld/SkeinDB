//! MANIFEST.log reader/writer (T010)
//!
//! The MANIFEST.log is an append-only sequence of records that describes the
//! current state of an LSM-style storage engine: which segment/run files exist,
//! what level they belong to, what the latest committed LSN is, and when the
//! engine was cleanly shut down. It is independent of the WAL: where the WAL
//! records logical row mutations, the MANIFEST records *file-level* events so
//! that recovery can rebuild the set of live files before applying any WAL
//! replay.
//!
//! File layout (on disk):
//!
//! ```text
//! +--------------------+
//! | FileHeader (64 B)  |   FileKind::Manifest
//! +--------------------+
//! | RecordFrame #0     |   varu-encoded ManifestRecord (see below)
//! +--------------------+
//! | RecordFrame #1     |
//! +--------------------+
//! | ...                |
//! +--------------------+
//! ```
//!
//! Each record payload begins with a 1-byte `rec_type` tag followed by
//! record-specific fields encoded using the shared `VarU` / `Bytes`
//! encodings from [`crate`]. The first bytes of the file are a
//! [`FileHeader`] with `file_kind == FileKind::Manifest`.
//!
//! The format is intentionally simple and forward-compatible: unknown
//! `rec_type` tags cause a decoder error at the record boundary, but the
//! surrounding `RecordFrame` machinery still CRC-checks and length-prefixes
//! each record, so partial writes at the tail of the file are recoverable
//! via torn-write truncation.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{
    append_record_frame, decode_varu, encode_varu, iter_record_frames, FileHeader, FileKind,
    FormatError, FILE_HEADER_LEN,
};

// ---- Record types -----------------------------------------------------------

/// Known kinds of files that can appear in the LSM storage tree.
///
/// Wire value is a single byte so that the tag-byte of a record is always
/// enough to find the sub-type without reaching into the record body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ManifestFileKind {
    Wal = 1,
    RowSeg = 2,
    ValSeg = 3,
    Run = 4,
}

impl ManifestFileKind {
    fn from_u8(v: u8) -> Result<Self, ManifestError> {
        match v {
            1 => Ok(ManifestFileKind::Wal),
            2 => Ok(ManifestFileKind::RowSeg),
            3 => Ok(ManifestFileKind::ValSeg),
            4 => Ok(ManifestFileKind::Run),
            _ => Err(ManifestError::UnknownFileKind(v)),
        }
    }
}

/// A single MANIFEST record.
///
/// Records are additive: the current engine state is computed by replaying
/// every record in order. `AddFile` adds a file to the live set; `RemoveFile`
/// removes it; `SetCurrentVersion` bumps a logical schema/version counter;
/// `SetLastLsn` records the highest durable WAL LSN; `CleanShutdown` is an
/// explicit anchor indicating that the previous process exited cleanly, so
/// WAL replay can start from `last_lsn + 1` safely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestRecord {
    /// Add a file of `kind` at LSM `level` with `file_id` to the live set.
    AddFile {
        kind: ManifestFileKind,
        file_id: u32,
        level: u32,
    },
    /// Remove a file from the live set.
    RemoveFile {
        kind: ManifestFileKind,
        file_id: u32,
    },
    /// Bump the engine's current-version counter (e.g. schema version).
    SetCurrentVersion { version: u64 },
    /// Record the highest durable WAL LSN.
    SetLastLsn { lsn: u64 },
    /// Anchor indicating the engine shut down cleanly at this point.
    CleanShutdown { unix_s: u64 },
}

// Tag bytes — stable and part of the on-disk format. Do not reorder.
const TAG_ADD_FILE: u8 = 0x01;
const TAG_REMOVE_FILE: u8 = 0x02;
const TAG_SET_CURRENT_VERSION: u8 = 0x03;
const TAG_SET_LAST_LSN: u8 = 0x04;
const TAG_CLEAN_SHUTDOWN: u8 = 0x05;

impl ManifestRecord {
    /// Encode a single record payload (without the surrounding `RecordFrame`
    /// length/CRC) for appending to a manifest.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match *self {
            ManifestRecord::AddFile {
                kind,
                file_id,
                level,
            } => {
                out.push(TAG_ADD_FILE);
                out.push(kind as u8);
                out.extend_from_slice(&encode_varu(file_id as u64));
                out.extend_from_slice(&encode_varu(level as u64));
            }
            ManifestRecord::RemoveFile { kind, file_id } => {
                out.push(TAG_REMOVE_FILE);
                out.push(kind as u8);
                out.extend_from_slice(&encode_varu(file_id as u64));
            }
            ManifestRecord::SetCurrentVersion { version } => {
                out.push(TAG_SET_CURRENT_VERSION);
                out.extend_from_slice(&encode_varu(version));
            }
            ManifestRecord::SetLastLsn { lsn } => {
                out.push(TAG_SET_LAST_LSN);
                out.extend_from_slice(&encode_varu(lsn));
            }
            ManifestRecord::CleanShutdown { unix_s } => {
                out.push(TAG_CLEAN_SHUTDOWN);
                out.extend_from_slice(&encode_varu(unix_s));
            }
        }
        out
    }

    /// Decode a single record payload previously produced by [`Self::encode`].
    pub fn decode(payload: &[u8]) -> Result<Self, ManifestError> {
        if payload.is_empty() {
            return Err(ManifestError::Format(FormatError::UnexpectedEof));
        }
        let tag = payload[0];
        let mut rest = &payload[1..];
        match tag {
            TAG_ADD_FILE => {
                if rest.is_empty() {
                    return Err(ManifestError::Format(FormatError::UnexpectedEof));
                }
                let kind = ManifestFileKind::from_u8(rest[0])?;
                rest = &rest[1..];
                let file_id = decode_varu(&mut rest).map_err(ManifestError::core)?;
                let level = decode_varu(&mut rest).map_err(ManifestError::core)?;
                let file_id = u32::try_from(file_id)
                    .map_err(|_| ManifestError::FieldOutOfRange("file_id"))?;
                let level =
                    u32::try_from(level).map_err(|_| ManifestError::FieldOutOfRange("level"))?;
                Ok(ManifestRecord::AddFile {
                    kind,
                    file_id,
                    level,
                })
            }
            TAG_REMOVE_FILE => {
                if rest.is_empty() {
                    return Err(ManifestError::Format(FormatError::UnexpectedEof));
                }
                let kind = ManifestFileKind::from_u8(rest[0])?;
                rest = &rest[1..];
                let file_id = decode_varu(&mut rest).map_err(ManifestError::core)?;
                let file_id = u32::try_from(file_id)
                    .map_err(|_| ManifestError::FieldOutOfRange("file_id"))?;
                Ok(ManifestRecord::RemoveFile { kind, file_id })
            }
            TAG_SET_CURRENT_VERSION => {
                let version = decode_varu(&mut rest).map_err(ManifestError::core)?;
                Ok(ManifestRecord::SetCurrentVersion { version })
            }
            TAG_SET_LAST_LSN => {
                let lsn = decode_varu(&mut rest).map_err(ManifestError::core)?;
                Ok(ManifestRecord::SetLastLsn { lsn })
            }
            TAG_CLEAN_SHUTDOWN => {
                let unix_s = decode_varu(&mut rest).map_err(ManifestError::core)?;
                Ok(ManifestRecord::CleanShutdown { unix_s })
            }
            other => Err(ManifestError::UnknownRecordType(other)),
        }
    }
}

/// Errors raised by the manifest module.
#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("manifest file does not start with a valid FileHeader of kind Manifest")]
    BadHeader,
    #[error("unknown record type {0:#04x}")]
    UnknownRecordType(u8),
    #[error("unknown file kind {0}")]
    UnknownFileKind(u8),
    #[error("field {0} is out of the supported range")]
    FieldOutOfRange(&'static str),
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl ManifestError {
    fn core(err: crate::CoreError) -> Self {
        match err {
            crate::CoreError::UnexpectedEof => ManifestError::Format(FormatError::UnexpectedEof),
            crate::CoreError::InvalidVarint => ManifestError::FieldOutOfRange("varu"),
        }
    }
}

// ---- Derived current state --------------------------------------------------

/// Fully-qualified identifier for a live file in the LSM tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ManifestFileRef {
    pub kind: ManifestFileKind,
    pub file_id: u32,
}

/// Current, replayed state of the manifest.
///
/// Computed by [`ManifestReader::replay_state`] or
/// [`ManifestWriter::state`]. This is the minimum set of facts a storage
/// engine needs before opening the WAL/segments on startup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestState {
    /// All currently live files and their levels. Level information is only
    /// tracked for `Run` files; other kinds default to level 0.
    pub files: Vec<ManifestFileEntry>,
    /// Highest schema / logical version the engine has committed.
    pub current_version: u64,
    /// Highest durable WAL LSN.
    pub last_lsn: u64,
    /// Unix timestamp of the last `CleanShutdown`, or `0` if the last
    /// recovery was implicit (crash, or first boot).
    pub last_clean_shutdown_unix_s: u64,
}

/// A live file entry in a replayed [`ManifestState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestFileEntry {
    pub file_ref: ManifestFileRef,
    pub level: u32,
}

impl ManifestState {
    /// Apply a single record, mutating this state in place.
    pub fn apply(&mut self, rec: &ManifestRecord) {
        match *rec {
            ManifestRecord::AddFile {
                kind,
                file_id,
                level,
            } => {
                let file_ref = ManifestFileRef { kind, file_id };
                // Most recent add for a given (kind, file_id) wins for level.
                if let Some(existing) = self.files.iter_mut().find(|e| e.file_ref == file_ref) {
                    existing.level = level;
                } else {
                    self.files.push(ManifestFileEntry { file_ref, level });
                }
            }
            ManifestRecord::RemoveFile { kind, file_id } => {
                let file_ref = ManifestFileRef { kind, file_id };
                self.files.retain(|e| e.file_ref != file_ref);
            }
            ManifestRecord::SetCurrentVersion { version } => {
                self.current_version = version;
            }
            ManifestRecord::SetLastLsn { lsn } => {
                self.last_lsn = lsn;
            }
            ManifestRecord::CleanShutdown { unix_s } => {
                self.last_clean_shutdown_unix_s = unix_s;
            }
        }
    }
}

// ---- Writer -----------------------------------------------------------------

/// Append-only MANIFEST writer.
///
/// Opens (or creates) a manifest file at the given path. On creation, writes
/// a [`FileHeader`] of kind [`FileKind::Manifest`]. Subsequent calls to
/// [`Self::append`] flush each record as a CRC-protected `RecordFrame`
/// and advance an in-memory replayed [`ManifestState`] so callers can read
/// the current state without re-opening the file.
pub struct ManifestWriter {
    path: PathBuf,
    file: File,
    state: ManifestState,
}

impl ManifestWriter {
    /// Open an existing manifest file for append, or create a fresh one.
    ///
    /// If the file is newly created, a fresh [`FileHeader`] is written.
    /// Otherwise, the existing header is validated and the tail records
    /// replayed into [`Self::state`] so the writer can report the current
    /// state immediately.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref().to_path_buf();
        let exists = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;

        let state = if exists && file.metadata()?.len() >= FILE_HEADER_LEN as u64 {
            ManifestReader::open(&path)?.replay_state()?
        } else {
            let header = FileHeader::new(FileKind::Manifest, 0, now_unix_s());
            file.write_all(&header.encode())?;
            file.flush()?;
            ManifestState::default()
        };

        Ok(Self { path, file, state })
    }

    /// Append a record, flushing it to disk.
    pub fn append(&mut self, rec: &ManifestRecord) -> Result<(), ManifestError> {
        let payload = rec.encode();
        let mut frame = Vec::with_capacity(payload.len() + 8);
        append_record_frame(&mut frame, &payload)?;
        self.file.write_all(&frame)?;
        self.file.flush()?;
        self.state.apply(rec);
        Ok(())
    }

    /// Force an fsync on the underlying file.
    pub fn sync(&self) -> Result<(), ManifestError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Current replayed state (including all records appended so far by
    /// this writer).
    pub fn state(&self) -> &ManifestState {
        &self.state
    }

    /// The filesystem path backing this manifest.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ---- Reader -----------------------------------------------------------------

/// Read-only MANIFEST reader.
pub struct ManifestReader {
    reader: BufReader<File>,
}

impl ManifestReader {
    /// Open a manifest file for reading. Validates the [`FileHeader`].
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let mut file = OpenOptions::new().read(true).open(path.as_ref())?;
        let mut header_bytes = [0u8; FILE_HEADER_LEN];
        file.read_exact(&mut header_bytes)
            .map_err(|_| ManifestError::BadHeader)?;
        let header = FileHeader::decode(&header_bytes)?;
        if header.file_kind != FileKind::Manifest {
            return Err(ManifestError::BadHeader);
        }
        file.seek(SeekFrom::Start(FILE_HEADER_LEN as u64))?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    /// Read all remaining records into memory.
    pub fn read_all(mut self) -> Result<Vec<ManifestRecord>, ManifestError> {
        let mut tail = Vec::new();
        self.reader.read_to_end(&mut tail)?;
        let mut records = Vec::new();
        for item in iter_record_frames(&tail) {
            let payload = item?;
            let rec = ManifestRecord::decode(&payload)?;
            records.push(rec);
        }
        Ok(records)
    }

    /// Read all records and apply them to a fresh [`ManifestState`].
    pub fn replay_state(self) -> Result<ManifestState, ManifestError> {
        let records = self.read_all()?;
        let mut state = ManifestState::default();
        for rec in &records {
            state.apply(rec);
        }
        Ok(state)
    }
}

// ---- Helpers ----------------------------------------------------------------

fn now_unix_s() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip_all_variants() {
        let cases = vec![
            ManifestRecord::AddFile {
                kind: ManifestFileKind::Wal,
                file_id: 7,
                level: 0,
            },
            ManifestRecord::AddFile {
                kind: ManifestFileKind::Run,
                file_id: 12345,
                level: 3,
            },
            ManifestRecord::RemoveFile {
                kind: ManifestFileKind::RowSeg,
                file_id: 42,
            },
            ManifestRecord::SetCurrentVersion {
                version: 0xFFFF_FFFF_FFFF,
            },
            ManifestRecord::SetLastLsn {
                lsn: 987_654_321_000,
            },
            ManifestRecord::CleanShutdown {
                unix_s: 1_700_000_000,
            },
        ];
        for rec in &cases {
            let bytes = rec.encode();
            let decoded = ManifestRecord::decode(&bytes).expect("decode");
            assert_eq!(&decoded, rec);
        }
    }

    #[test]
    fn decode_rejects_unknown_record_type() {
        let bogus = vec![0x7F, 0, 0, 0];
        let err = ManifestRecord::decode(&bogus).unwrap_err();
        matches!(err, ManifestError::UnknownRecordType(0x7F));
    }

    #[test]
    fn decode_rejects_unknown_file_kind() {
        let bogus = vec![TAG_ADD_FILE, 99, 0];
        let err = ManifestRecord::decode(&bogus).unwrap_err();
        matches!(err, ManifestError::UnknownFileKind(99));
    }

    #[test]
    fn state_apply_add_then_remove() {
        let mut st = ManifestState::default();
        st.apply(&ManifestRecord::AddFile {
            kind: ManifestFileKind::Run,
            file_id: 1,
            level: 2,
        });
        st.apply(&ManifestRecord::AddFile {
            kind: ManifestFileKind::Run,
            file_id: 2,
            level: 0,
        });
        st.apply(&ManifestRecord::RemoveFile {
            kind: ManifestFileKind::Run,
            file_id: 1,
        });
        assert_eq!(st.files.len(), 1);
        assert_eq!(st.files[0].file_ref.file_id, 2);
        assert_eq!(st.files[0].level, 0);
    }

    #[test]
    fn state_apply_add_updates_level() {
        let mut st = ManifestState::default();
        st.apply(&ManifestRecord::AddFile {
            kind: ManifestFileKind::Run,
            file_id: 5,
            level: 1,
        });
        st.apply(&ManifestRecord::AddFile {
            kind: ManifestFileKind::Run,
            file_id: 5,
            level: 3,
        });
        assert_eq!(st.files.len(), 1);
        assert_eq!(st.files[0].level, 3);
    }

    fn temp_path(label: &str) -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut p = std::env::temp_dir();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!(
            "skeindb_core_manifest_{}_{}_{}",
            label,
            std::process::id(),
            ts
        ));
        p
    }

    #[test]
    fn writer_reader_roundtrip_via_file() -> Result<(), ManifestError> {
        let path = temp_path("rw_roundtrip");
        {
            let mut w = ManifestWriter::open(&path)?;
            w.append(&ManifestRecord::AddFile {
                kind: ManifestFileKind::Wal,
                file_id: 1,
                level: 0,
            })?;
            w.append(&ManifestRecord::SetLastLsn { lsn: 42 })?;
            w.append(&ManifestRecord::AddFile {
                kind: ManifestFileKind::Run,
                file_id: 101,
                level: 2,
            })?;
            w.append(&ManifestRecord::SetCurrentVersion { version: 7 })?;
            w.append(&ManifestRecord::CleanShutdown {
                unix_s: 1_234_567_890,
            })?;
            w.sync()?;
        }

        let records = ManifestReader::open(&path)?.read_all()?;
        assert_eq!(records.len(), 5);

        let state = ManifestReader::open(&path)?.replay_state()?;
        assert_eq!(state.last_lsn, 42);
        assert_eq!(state.current_version, 7);
        assert_eq!(state.last_clean_shutdown_unix_s, 1_234_567_890);
        assert_eq!(state.files.len(), 2);

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn writer_reopen_replays_existing_state() -> Result<(), ManifestError> {
        let path = temp_path("reopen_replay");
        {
            let mut w = ManifestWriter::open(&path)?;
            w.append(&ManifestRecord::AddFile {
                kind: ManifestFileKind::Run,
                file_id: 11,
                level: 0,
            })?;
            w.append(&ManifestRecord::SetLastLsn { lsn: 500 })?;
        }
        let mut w2 = ManifestWriter::open(&path)?;
        assert_eq!(w2.state().last_lsn, 500);
        assert_eq!(w2.state().files.len(), 1);
        w2.append(&ManifestRecord::SetLastLsn { lsn: 501 })?;
        assert_eq!(w2.state().last_lsn, 501);

        let replayed = ManifestReader::open(&path)?.replay_state()?;
        assert_eq!(replayed.last_lsn, 501);
        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn reader_rejects_non_manifest_file_kind() -> Result<(), ManifestError> {
        let path = temp_path("bad_kind");
        let header = FileHeader::new(FileKind::Wal, 0, now_unix_s());
        std::fs::write(&path, header.encode())?;
        let err = match ManifestReader::open(&path) {
            Ok(_) => panic!("expected non-manifest header to be rejected"),
            Err(err) => err,
        };
        matches!(err, ManifestError::BadHeader);
        let _ = std::fs::remove_file(path);
        Ok(())
    }
}
