//! Sorted runs (`.run`) + simple level0 LSM (T013).
//!
//! This module implements the immutable `.run` file format described in
//! `docs/ON_DISK_FORMAT.md` together with a minimal memtable + level0 LSM.
//! The scope is intentionally compact:
//!
//! - `RunWriter` builds immutable sorted files from strictly increasing keys.
//! - `RunReader` loads the index/footer and supports full scans + point lookups.
//! - `SimpleLsm` keeps a memtable in memory and flushes it to new level0 run
//!   files, reading level0 newest-first on lookups.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{
    crc32c, decode_varu, encode_varu, CoreError, FileHeader, FileKind, FormatError, FILE_HEADER_LEN,
};

const DATA_BLOCK_TYPE: u8 = 0x40;
const INDEX_BLOCK_TYPE: u8 = 0x41;
const BLOCK_VERSION: u8 = 1;
const RUN_FOOTER_MAGIC: [u8; 8] = *b"SKNRUN\0\x01";
const RUN_FOOTER_LEN: usize = 8 + 8 + 4;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("run file does not start with a valid FileHeader of kind Run")]
    BadHeader,
    #[error("invalid run footer")]
    BadFooter,
    #[error("invalid block type {0:#04x}")]
    InvalidBlockType(u8),
    #[error("unsupported block version {0}")]
    UnsupportedBlockVersion(u8),
    #[error("invalid run block: {0}")]
    InvalidBlock(&'static str),
    #[error("run keys must be appended in strictly increasing order")]
    KeysNotStrictlyIncreasing,
    #[error("run writer is already finished")]
    WriterFinished,
    #[error("invalid run file name; expected run-######.run")]
    InvalidRunFileName,
    #[error("footer CRC mismatch (expected {expected:#010x}, got {actual:#010x})")]
    FooterCrcMismatch { expected: u32, actual: u32 },
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Core(#[from] CoreError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunIndexEntry {
    pub first_key: Vec<u8>,
    pub block_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFooter {
    pub index_offset: u64,
    pub file_crc32c: u32,
}

impl RunFooter {
    fn encode(&self) -> [u8; RUN_FOOTER_LEN] {
        let mut out = [0u8; RUN_FOOTER_LEN];
        out[0..8].copy_from_slice(&RUN_FOOTER_MAGIC);
        out[8..16].copy_from_slice(&self.index_offset.to_le_bytes());
        out[16..20].copy_from_slice(&self.file_crc32c.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, RunError> {
        if bytes.len() < RUN_FOOTER_LEN {
            return Err(RunError::BadFooter);
        }
        if bytes[0..8] != RUN_FOOTER_MAGIC {
            return Err(RunError::BadFooter);
        }
        let mut index_offset = [0u8; 8];
        index_offset.copy_from_slice(&bytes[8..16]);
        let mut file_crc32c = [0u8; 4];
        file_crc32c.copy_from_slice(&bytes[16..20]);
        Ok(Self {
            index_offset: u64::from_le_bytes(index_offset),
            file_crc32c: u32::from_le_bytes(file_crc32c),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RunWriterOptions {
    pub target_block_size: usize,
}

impl Default for RunWriterOptions {
    fn default() -> Self {
        Self {
            target_block_size: 4 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMetadata {
    pub path: PathBuf,
    pub entry_count: usize,
    pub block_count: usize,
    pub index_offset: u64,
    pub file_crc32c: u32,
    pub file_size: u64,
}

pub struct RunWriter {
    path: PathBuf,
    file: File,
    options: RunWriterOptions,
    pending_entries: Vec<RunEntry>,
    pending_bytes: usize,
    index: Vec<RunIndexEntry>,
    current_offset: u64,
    last_key: Option<Vec<u8>>,
    entry_count: usize,
    finished: bool,
}

impl RunWriter {
    pub fn create(path: impl AsRef<Path>, options: RunWriterOptions) -> Result<Self, RunError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&FileHeader::new(FileKind::Run, 0, now_unix_s()).encode())?;
        file.flush()?;
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            path,
            file,
            options,
            pending_entries: Vec::new(),
            pending_bytes: 0,
            index: Vec::new(),
            current_offset: FILE_HEADER_LEN as u64,
            last_key: None,
            entry_count: 0,
            finished: false,
        })
    }

    pub fn append(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), RunError> {
        if self.finished {
            return Err(RunError::WriterFinished);
        }
        if let Some(last_key) = &self.last_key {
            if key <= *last_key {
                return Err(RunError::KeysNotStrictlyIncreasing);
            }
        }
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(estimate_entry_bytes(&key, &value));
        self.pending_entries.push(RunEntry {
            key: key.clone(),
            value,
        });
        self.last_key = Some(key);
        self.entry_count = self.entry_count.saturating_add(1);

        if self.pending_bytes >= self.options.target_block_size.max(256) {
            self.flush_pending_block()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<RunMetadata, RunError> {
        if self.finished {
            return Err(RunError::WriterFinished);
        }
        self.flush_pending_block()?;

        let index_offset = self.current_offset;
        let index_block = encode_index_block(&self.index)?;
        self.file.write_all(&index_block)?;
        self.current_offset = self.current_offset.saturating_add(index_block.len() as u64);
        self.file.flush()?;

        let pre_footer = fs::read(&self.path)?;
        let footer = RunFooter {
            index_offset,
            file_crc32c: crc32c(&pre_footer),
        };
        self.file.write_all(&footer.encode())?;
        self.file.flush()?;
        self.finished = true;

        Ok(RunMetadata {
            path: self.path,
            entry_count: self.entry_count,
            block_count: self.index.len(),
            index_offset,
            file_crc32c: footer.file_crc32c,
            file_size: self.current_offset.saturating_add(RUN_FOOTER_LEN as u64),
        })
    }

    fn flush_pending_block(&mut self) -> Result<(), RunError> {
        if self.pending_entries.is_empty() {
            return Ok(());
        }
        let first_key = self
            .pending_entries
            .first()
            .map(|e| e.key.clone())
            .ok_or(RunError::InvalidBlock("pending block missing first key"))?;
        let payload = encode_data_block(&self.pending_entries)?;
        self.file.write_all(&payload)?;
        self.index.push(RunIndexEntry {
            first_key,
            block_offset: self.current_offset,
        });
        self.current_offset = self.current_offset.saturating_add(payload.len() as u64);
        self.pending_entries.clear();
        self.pending_bytes = 0;
        Ok(())
    }
}

pub struct RunReader {
    path: PathBuf,
    index_offset: u64,
    footer: RunFooter,
    index: Vec<RunIndexEntry>,
}

impl RunReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RunError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).open(&path)?;
        validate_run_header(&mut file)?;
        let len = file.metadata()?.len() as usize;
        if len < FILE_HEADER_LEN + RUN_FOOTER_LEN {
            return Err(RunError::BadFooter);
        }

        let mut footer_bytes = [0u8; RUN_FOOTER_LEN];
        file.seek(SeekFrom::Start((len - RUN_FOOTER_LEN) as u64))?;
        file.read_exact(&mut footer_bytes)?;
        let footer = RunFooter::decode(&footer_bytes)?;
        if footer.index_offset < FILE_HEADER_LEN as u64
            || footer.index_offset as usize > len.saturating_sub(RUN_FOOTER_LEN)
        {
            return Err(RunError::BadFooter);
        }

        let file_bytes = fs::read(&path)?;
        let expected_crc = footer.file_crc32c;
        if expected_crc != 0 {
            let actual_crc = crc32c(&file_bytes[..len - RUN_FOOTER_LEN]);
            if expected_crc != actual_crc {
                return Err(RunError::FooterCrcMismatch {
                    expected: expected_crc,
                    actual: actual_crc,
                });
            }
        }

        let index = decode_index_block(
            &file_bytes[footer.index_offset as usize..len.saturating_sub(RUN_FOOTER_LEN)],
        )?;
        for window in index.windows(2) {
            if window[0].block_offset >= window[1].block_offset {
                return Err(RunError::InvalidBlock(
                    "index block offsets must be strictly increasing",
                ));
            }
        }
        if let Some(last) = index.last() {
            if last.block_offset >= footer.index_offset {
                return Err(RunError::BadFooter);
            }
        }

        Ok(Self {
            path,
            index_offset: footer.index_offset,
            footer,
            index,
        })
    }

    pub fn read_all(&self) -> Result<Vec<RunEntry>, RunError> {
        let mut out = Vec::new();
        for block_idx in 0..self.index.len() {
            let entries = self.read_block_entries(block_idx)?;
            out.extend(entries);
        }
        Ok(out)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RunError> {
        let Some(block_idx) = self.find_block_for_key(key) else {
            return Ok(None);
        };
        for entry in self.read_block_entries(block_idx)? {
            match entry.key.as_slice().cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(entry.value)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }

    pub fn index(&self) -> &[RunIndexEntry] {
        &self.index
    }

    pub fn index_offset(&self) -> u64 {
        self.index_offset
    }

    pub fn footer(&self) -> &RunFooter {
        &self.footer
    }

    fn find_block_for_key(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() {
            return None;
        }
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.index[mid].first_key.as_slice() <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.checked_sub(1)
    }

    fn read_block_entries(&self, idx: usize) -> Result<Vec<RunEntry>, RunError> {
        let start = self
            .index
            .get(idx)
            .map(|e| e.block_offset)
            .ok_or(RunError::InvalidBlock("missing data block index entry"))?
            as usize;
        let end = if idx + 1 < self.index.len() {
            self.index[idx + 1].block_offset as usize
        } else {
            self.index_offset as usize
        };
        let bytes = fs::read(&self.path)?;
        if end <= start || end > bytes.len() {
            return Err(RunError::InvalidBlock("data block offset range is invalid"));
        }
        decode_data_block(&bytes[start..end])
    }
}

#[derive(Debug, Clone)]
pub struct SimpleLsmConfig {
    pub target_block_size: usize,
    pub memtable_max_entries: usize,
}

impl Default for SimpleLsmConfig {
    fn default() -> Self {
        Self {
            target_block_size: 4 * 1024,
            memtable_max_entries: 64,
        }
    }
}

pub struct SimpleLsm {
    dir: PathBuf,
    config: SimpleLsmConfig,
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
    level0_runs: Vec<PathBuf>,
    next_run_id: u32,
}

impl SimpleLsm {
    pub fn open(dir: impl AsRef<Path>, config: SimpleLsmConfig) -> Result<Self, RunError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let mut discovered = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("run") {
                continue;
            }
            let run_id = parse_run_id(&path)?;
            discovered.push((run_id, path));
        }
        discovered.sort_by_key(|(id, _)| *id);
        let next_run_id = discovered.last().map(|(id, _)| id + 1).unwrap_or(1);

        Ok(Self {
            dir,
            config,
            memtable: BTreeMap::new(),
            level0_runs: discovered.into_iter().map(|(_, path)| path).collect(),
            next_run_id,
        })
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<PathBuf>, RunError> {
        self.memtable.insert(key, value);
        if self.config.memtable_max_entries > 0
            && self.memtable.len() >= self.config.memtable_max_entries
        {
            return self.flush_memtable();
        }
        Ok(None)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RunError> {
        if let Some(value) = self.memtable.get(key) {
            return Ok(Some(value.clone()));
        }
        for path in self.level0_runs.iter().rev() {
            let reader = RunReader::open(path)?;
            if let Some(value) = reader.get(key)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub fn flush_memtable(&mut self) -> Result<Option<PathBuf>, RunError> {
        if self.memtable.is_empty() {
            return Ok(None);
        }
        let path = self
            .dir
            .join(format!("run-{run_id:06}.run", run_id = self.next_run_id));
        let mut writer = RunWriter::create(
            &path,
            RunWriterOptions {
                target_block_size: self.config.target_block_size,
            },
        )?;
        for (key, value) in self.memtable.iter() {
            writer.append(key.clone(), value.clone())?;
        }
        let _ = writer.finish()?;
        self.memtable.clear();
        self.level0_runs.push(path.clone());
        self.next_run_id = self.next_run_id.saturating_add(1);
        Ok(Some(path))
    }

    pub fn memtable_len(&self) -> usize {
        self.memtable.len()
    }

    pub fn level0_run_count(&self) -> usize {
        self.level0_runs.len()
    }

    pub fn level0_paths(&self) -> &[PathBuf] {
        &self.level0_runs
    }

    pub fn next_run_id(&self) -> u32 {
        self.next_run_id
    }
}

fn validate_run_header(file: &mut File) -> Result<(), RunError> {
    let len = file.metadata()?.len();
    if len < FILE_HEADER_LEN as u64 {
        return Err(RunError::BadHeader);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut header_bytes = [0u8; FILE_HEADER_LEN];
    file.read_exact(&mut header_bytes)
        .map_err(|_| RunError::BadHeader)?;
    let header = FileHeader::decode(&header_bytes)?;
    if header.file_kind != FileKind::Run {
        return Err(RunError::BadHeader);
    }
    Ok(())
}

fn encode_data_block(entries: &[RunEntry]) -> Result<Vec<u8>, RunError> {
    let mut out = vec![DATA_BLOCK_TYPE, BLOCK_VERSION];
    out.extend_from_slice(&encode_varu(entries.len() as u64));
    for entry in entries {
        out.extend_from_slice(&encode_varu(entry.key.len() as u64));
        out.extend_from_slice(&entry.key);
        out.extend_from_slice(&encode_varu(entry.value.len() as u64));
        out.extend_from_slice(&entry.value);
    }
    Ok(out)
}

fn decode_data_block(bytes: &[u8]) -> Result<Vec<RunEntry>, RunError> {
    if bytes.len() < 2 {
        return Err(RunError::InvalidBlock("data block is shorter than header"));
    }
    if bytes[0] != DATA_BLOCK_TYPE {
        return Err(RunError::InvalidBlockType(bytes[0]));
    }
    if bytes[1] != BLOCK_VERSION {
        return Err(RunError::UnsupportedBlockVersion(bytes[1]));
    }
    let mut rest = &bytes[2..];
    let entry_count = decode_varu(&mut rest)? as usize;
    let mut entries = Vec::with_capacity(entry_count);
    let mut last_key: Option<Vec<u8>> = None;
    for _ in 0..entry_count {
        let key_len = decode_varu(&mut rest)? as usize;
        if rest.len() < key_len {
            return Err(RunError::InvalidBlock(
                "data block key exceeds remaining bytes",
            ));
        }
        let key = rest[..key_len].to_vec();
        rest = &rest[key_len..];

        let value_len = decode_varu(&mut rest)? as usize;
        if rest.len() < value_len {
            return Err(RunError::InvalidBlock(
                "data block value exceeds remaining bytes",
            ));
        }
        let value = rest[..value_len].to_vec();
        rest = &rest[value_len..];

        if let Some(last) = &last_key {
            if key <= *last {
                return Err(RunError::InvalidBlock(
                    "data block keys must be strictly increasing",
                ));
            }
        }
        last_key = Some(key.clone());
        entries.push(RunEntry { key, value });
    }
    if !rest.is_empty() {
        return Err(RunError::InvalidBlock("trailing bytes after data block"));
    }
    Ok(entries)
}

fn encode_index_block(index: &[RunIndexEntry]) -> Result<Vec<u8>, RunError> {
    let mut out = vec![INDEX_BLOCK_TYPE, BLOCK_VERSION];
    out.extend_from_slice(&encode_varu(index.len() as u64));
    for entry in index {
        out.extend_from_slice(&encode_varu(entry.first_key.len() as u64));
        out.extend_from_slice(&entry.first_key);
        out.extend_from_slice(&entry.block_offset.to_le_bytes());
    }
    Ok(out)
}

fn decode_index_block(bytes: &[u8]) -> Result<Vec<RunIndexEntry>, RunError> {
    if bytes.len() < 2 {
        return Err(RunError::InvalidBlock("index block is shorter than header"));
    }
    if bytes[0] != INDEX_BLOCK_TYPE {
        return Err(RunError::InvalidBlockType(bytes[0]));
    }
    if bytes[1] != BLOCK_VERSION {
        return Err(RunError::UnsupportedBlockVersion(bytes[1]));
    }
    let mut rest = &bytes[2..];
    let block_count = decode_varu(&mut rest)? as usize;
    let mut index = Vec::with_capacity(block_count);
    let mut last_key: Option<Vec<u8>> = None;
    for _ in 0..block_count {
        let key_len = decode_varu(&mut rest)? as usize;
        if rest.len() < key_len + 8 {
            return Err(RunError::InvalidBlock(
                "index block entry exceeds remaining bytes",
            ));
        }
        let first_key = rest[..key_len].to_vec();
        rest = &rest[key_len..];
        let mut offset_bytes = [0u8; 8];
        offset_bytes.copy_from_slice(&rest[..8]);
        rest = &rest[8..];

        if let Some(last) = &last_key {
            if first_key <= *last {
                return Err(RunError::InvalidBlock(
                    "index block first keys must be strictly increasing",
                ));
            }
        }
        last_key = Some(first_key.clone());
        index.push(RunIndexEntry {
            first_key,
            block_offset: u64::from_le_bytes(offset_bytes),
        });
    }
    if !rest.is_empty() {
        return Err(RunError::InvalidBlock("trailing bytes after index block"));
    }
    Ok(index)
}

fn estimate_entry_bytes(key: &[u8], value: &[u8]) -> usize {
    2 + key.len() + value.len() + 20
}

fn parse_run_id(path: &Path) -> Result<u32, RunError> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or(RunError::InvalidRunFileName)?;
    let suffix = stem
        .strip_prefix("run-")
        .ok_or(RunError::InvalidRunFileName)?;
    suffix
        .parse::<u32>()
        .map_err(|_| RunError::InvalidRunFileName)
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
            "skeindb_core_run_{}_{}_{}",
            label,
            std::process::id(),
            ts
        ));
        p
    }

    #[test]
    fn run_writer_rejects_out_of_order_keys() -> Result<(), RunError> {
        let path = temp_path("writer_order");
        let mut writer = RunWriter::create(
            &path,
            RunWriterOptions {
                target_block_size: 256,
            },
        )?;
        writer.append(b"b".to_vec(), b"1".to_vec())?;
        let err = writer
            .append(b"a".to_vec(), b"2".to_vec())
            .expect_err("out of order append should fail");
        match err {
            RunError::KeysNotStrictlyIncreasing => {}
            other => panic!("unexpected error: {other}"),
        }
        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn run_reader_rejects_bad_header() -> Result<(), RunError> {
        let path = temp_path("bad_header");
        std::fs::write(
            &path,
            FileHeader::new(FileKind::Wal, 0, now_unix_s()).encode(),
        )?;
        let err = match RunReader::open(&path) {
            Ok(_) => panic!("expected bad header"),
            Err(err) => err,
        };
        match err {
            RunError::BadHeader => {}
            other => panic!("unexpected error: {other}"),
        }
        let _ = std::fs::remove_file(path);
        Ok(())
    }
}
