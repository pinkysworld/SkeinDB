use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::{
    append_record_frame, decode_record_frame, decode_varu, encode_varu, value_id, CoreError,
    FileHeader, FileKind, FormatError, ValueKind, FILE_HEADER_LEN,
};

pub type ValueId = [u8; 16];

const VSEG_RECORD_TYPE: u8 = 0x20;
const VSEG_RECORD_VERSION: u8 = 1;
const DELTA1_CODEC_COPY_ADD: u8 = 0;

#[derive(Debug, Error)]
pub enum ValueStoreError {
    #[error("value not found")]
    NotFound,
    #[error("value segment file does not start with a valid FileHeader of kind ValSeg")]
    BadHeader,
    #[error("invalid value segment record type {0:#04x}")]
    InvalidSegmentRecordType(u8),
    #[error("unsupported value segment record version {0}")]
    UnsupportedSegmentRecordVersion(u8),
    #[error("unsupported value segment codec {0}")]
    UnsupportedValueCodec(u8),
    #[error("unsupported delta codec {0}")]
    UnsupportedDeltaCodec(u8),
    #[error("invalid value kind {0}")]
    InvalidValueKind(u8),
    #[error("duplicate ValueId encountered while loading a value segment")]
    DuplicateValueId,
    #[error("invalid value segment entry: {0}")]
    InvalidSegmentEntry(&'static str),
    #[error("invalid delta patch")]
    InvalidDelta,
    #[error("delta reconstruction length mismatch")]
    DeltaLengthMismatch,
    #[error("delta base missing")]
    MissingDeltaBase,
    #[error(transparent)]
    Format(#[from] FormatError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("core decode error: {0}")]
    Core(#[from] CoreError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueSegmentCodec {
    Raw = 0,
    Zstd = 1,
}

impl ValueSegmentCodec {
    fn from_u8(v: u8) -> Result<Self, ValueStoreError> {
        match v {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Zstd),
            _ => Err(ValueStoreError::UnsupportedValueCodec(v)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValueSegmentEntry {
    pub id: ValueId,
    pub entry: ValueEntry,
}

#[derive(Debug, Clone)]
pub struct ValueEntry {
    pub kind: ValueKind,
    pub bytes: Vec<u8>,
    pub delta: Option<DeltaEntry>,
}

#[derive(Debug, Clone)]
pub struct DeltaEntry {
    pub base: ValueId,
    pub full_len: usize,
    pub depth: usize,
    pub skip: Vec<SkipPatch>,
}

#[derive(Debug, Clone)]
pub struct SkipPatch {
    pub base: ValueId,
    pub patch: Vec<u8>,
    pub distance: usize,
}

#[derive(Debug, Clone)]
pub struct DeltaPolicy {
    pub enabled: bool,
    pub min_bytes: usize,
    pub max_chain: usize,
    pub min_savings_ratio: f64,
    pub snapshot_interval: usize,
    pub max_skip: usize,
}

impl Default for DeltaPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            min_bytes: 128,
            max_chain: 4,
            min_savings_ratio: 0.7,
            snapshot_interval: 0,
            max_skip: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValueStoreConfig {
    pub enable_learned_index: bool,
    pub segment_size: usize,
    pub refresh_policy: ModelRefreshPolicy,
    pub delta_policy: DeltaPolicy,
}

impl Default for ValueStoreConfig {
    fn default() -> Self {
        Self {
            enable_learned_index: false,
            segment_size: 64,
            refresh_policy: ModelRefreshPolicy::default(),
            delta_policy: DeltaPolicy::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModelRefreshPolicy {
    pub min_samples: usize,
    pub max_inserts: usize,
    pub max_shift_score: f64,
}

impl Default for ModelRefreshPolicy {
    fn default() -> Self {
        Self {
            min_samples: 256,
            max_inserts: 4096,
            max_shift_score: 0.25,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ValueStoreStats {
    pub entries: usize,
    pub lookups: u64,
    pub learned_hits: u64,
    pub fallback_hits: u64,
    pub probes: u64,
    pub delta_values_written: u64,
    pub delta_bytes_saved_estimate: u64,
    pub avg_delta_chain_depth: f64,
    pub avg_delta_reconstruct_steps: f64,
}

impl ValueStoreStats {
    pub fn learned_hit_rate(&self) -> f64 {
        if self.lookups == 0 {
            return 0.0;
        }
        self.learned_hits as f64 / self.lookups as f64
    }

    pub fn avg_probes(&self) -> f64 {
        if self.lookups == 0 {
            return 0.0;
        }
        self.probes as f64 / self.lookups as f64
    }
}

#[derive(Debug, Default, Clone)]
struct DeltaStats {
    values_written: u64,
    bytes_saved: u64,
    total_depth: u64,
    reconstruct_calls: u64,
    reconstruct_steps: u64,
}

#[derive(Debug, Clone)]
pub struct DeltaReconstructTrace {
    pub steps: usize,
    pub used_skip: bool,
}

#[derive(Debug, Clone)]
pub struct DeltaBenchmark {
    pub values: usize,
    pub p50_steps: usize,
    pub p99_steps: usize,
    pub p999_steps: usize,
    pub avg_steps: f64,
}

#[derive(Debug, Default, Clone)]
pub struct EnergyEstimate {
    pub cpu_units: u64,
    pub io_bytes_read: u64,
    pub io_bytes_written: u64,
    pub energy_score: f64,
}

impl EnergyEstimate {
    fn from_counts(cpu_units: u64, io_bytes_read: u64, io_bytes_written: u64) -> Self {
        let energy_score = cpu_units as f64 + io_bytes_read as f64 + io_bytes_written as f64;
        Self {
            cpu_units,
            io_bytes_read,
            io_bytes_written,
            energy_score,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeltaCompactionReport {
    pub rewritten: usize,
    pub skip_rebuilt: usize,
    pub energy: EnergyEstimate,
}

/// Topology analysis report for delta chain optimization (R03).
#[derive(Debug, Clone)]
pub struct DeltaTopologyReport {
    pub total_entries: usize,
    pub cell_entries: usize,
    pub delta_entries: usize,
    pub avg_depth: f64,
    pub max_depth: usize,
    pub p50_depth: usize,
    pub p99_depth: usize,
    pub max_fanout: usize,
    pub avg_fanout: f64,
    pub skip_patch_count: usize,
    pub total_cell_bytes: u64,
    pub total_delta_bytes: u64,
    pub savings_ratio: f64,
    pub hot_chains: Vec<HotChainInfo>,
}

/// Information about a delta chain that may need optimization.
#[derive(Debug, Clone)]
pub struct HotChainInfo {
    pub id: ValueId,
    pub depth: usize,
    pub skip_patches: usize,
    pub bytes: usize,
    pub needs_compaction: bool,
    pub needs_skip_patch: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct LookupTrace {
    pub probes: usize,
    pub used_learned: bool,
    pub used_fallback: bool,
}

#[derive(Debug, Clone)]
pub struct LookupBenchmark {
    pub lookups: usize,
    pub p50_probes: usize,
    pub p99_probes: usize,
    pub p999_probes: usize,
    pub memory_bytes: usize,
    pub learned_hit_rate: f64,
}

#[derive(Debug, Clone)]
pub struct ValueIdHistogram {
    buckets: [u64; 256],
    total: u64,
}

impl Default for ValueIdHistogram {
    fn default() -> Self {
        Self {
            buckets: [0; 256],
            total: 0,
        }
    }
}

impl ValueIdHistogram {
    pub fn record(&mut self, id: &ValueId) {
        let idx = id[0] as usize;
        self.buckets[idx] = self.buckets[idx].saturating_add(1);
        self.total = self.total.saturating_add(1);
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn bucket(&self, idx: usize) -> u64 {
        self.buckets[idx]
    }

    pub fn l1_distance(&self, other: &Self) -> f64 {
        if self.total == 0 || other.total == 0 {
            return 0.0;
        }

        let mut sum = 0.0;
        for i in 0..256 {
            let a = self.buckets[i] as f64 / self.total as f64;
            let b = other.buckets[i] as f64 / other.total as f64;
            sum += (a - b).abs();
        }
        sum
    }
}

/// Bloom filter for probabilistic ValueID existence checks (T165).
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    m_bits: usize,
    k_hashes: u32,
    count: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter sized for `expected` items with target FPR ~1%.
    pub fn new(expected: usize) -> Self {
        let m_bits = (expected.max(64) * 10).next_power_of_two();
        Self {
            bits: vec![0u64; m_bits / 64],
            m_bits,
            k_hashes: 7,
            count: 0,
        }
    }

    fn hashes(&self, id: &ValueId) -> [usize; 7] {
        // ValueId is 16 bytes (128 bits). Use different byte slices as hash seeds.
        let h1 = u64::from_le_bytes([id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7]]);
        let h2 = u64::from_le_bytes([id[8], id[9], id[10], id[11], id[12], id[13], id[14], id[15]]);
        let m = self.m_bits as u64;
        let mut out = [0usize; 7];
        for (i, slot) in out.iter_mut().enumerate() {
            let h = h1
                .wrapping_add((i as u64).wrapping_mul(h2))
                .wrapping_add(i as u64);
            *slot = (h % m) as usize;
        }
        out
    }

    pub fn insert(&mut self, id: &ValueId) {
        for pos in self.hashes(id) {
            self.bits[pos / 64] |= 1u64 << (pos % 64);
        }
        self.count += 1;
    }

    /// Returns true if the item *might* exist, false if it definitely does not.
    pub fn maybe_contains(&self, id: &ValueId) -> bool {
        for pos in self.hashes(id) {
            if self.bits[pos / 64] & (1u64 << (pos % 64)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Estimated false positive rate.
    pub fn estimated_fpr(&self) -> f64 {
        let set_bits: u32 = self.bits.iter().map(|w| w.count_ones()).sum();
        let fill = set_bits as f64 / self.m_bits as f64;
        fill.powi(self.k_hashes as i32)
    }
}

#[derive(Debug)]
pub struct ValueStore {
    entries: Vec<ValueEntry>,
    entry_ids: Vec<ValueId>,
    index: HashMap<ValueId, usize>,
    sorted_keys: Vec<ValueId>,
    sorted_entries: Vec<usize>,
    learned: Option<LearnedIndex>,
    bloom: BloomFilter,
    config: ValueStoreConfig,
    lookup_hist: ValueIdHistogram,
    model_hist: Option<ValueIdHistogram>,
    inserts_since_refresh: usize,
    stats: ValueStoreStats,
    delta_stats: DeltaStats,
}

impl ValueStore {
    pub fn new(config: ValueStoreConfig) -> Self {
        Self {
            entries: Vec::new(),
            entry_ids: Vec::new(),
            index: HashMap::new(),
            sorted_keys: Vec::new(),
            sorted_entries: Vec::new(),
            learned: None,
            bloom: BloomFilter::new(1024),
            config,
            lookup_hist: ValueIdHistogram::default(),
            model_hist: None,
            inserts_since_refresh: 0,
            stats: ValueStoreStats::default(),
            delta_stats: DeltaStats::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Check whether a ValueId exists in the store without mutating state.
    pub fn contains(&self, id: ValueId) -> bool {
        self.index.contains_key(&id)
    }

    /// Fast probabilistic check: returns false if definitely missing.
    pub fn bloom_maybe_contains(&self, id: &ValueId) -> bool {
        self.bloom.maybe_contains(id)
    }

    /// Bloom filter statistics for replication metrics.
    pub fn bloom_stats(&self) -> (usize, usize, f64) {
        (
            self.bloom.count(),
            self.bloom.size_bytes(),
            self.bloom.estimated_fpr(),
        )
    }

    pub fn stats(&self) -> ValueStoreStats {
        let mut stats = self.stats.clone();
        stats.entries = self.entries.len();
        stats.delta_values_written = self.delta_stats.values_written;
        stats.delta_bytes_saved_estimate = self.delta_stats.bytes_saved;
        stats.avg_delta_chain_depth = if self.delta_stats.values_written == 0 {
            0.0
        } else {
            self.delta_stats.total_depth as f64 / self.delta_stats.values_written as f64
        };
        stats.avg_delta_reconstruct_steps = if self.delta_stats.reconstruct_calls == 0 {
            0.0
        } else {
            self.delta_stats.reconstruct_steps as f64 / self.delta_stats.reconstruct_calls as f64
        };
        stats
    }

    pub fn delta_policy(&self) -> &DeltaPolicy {
        &self.config.delta_policy
    }

    pub fn set_delta_policy(&mut self, policy: DeltaPolicy) {
        self.config.delta_policy = policy;
    }

    pub fn lookup_histogram(&self) -> &ValueIdHistogram {
        &self.lookup_hist
    }

    pub fn put(&mut self, kind: ValueKind, bytes: Vec<u8>) -> ValueId {
        let id = value_id(&bytes);
        self.put_with_id(kind, id, bytes)
    }

    pub fn put_with_id(&mut self, kind: ValueKind, id: ValueId, bytes: Vec<u8>) -> ValueId {
        if self.index.contains_key(&id) {
            return id;
        }

        let idx = self.entries.len();
        self.entry_ids.push(id);
        self.entries.push(ValueEntry {
            kind,
            bytes,
            delta: None,
        });
        self.index.insert(id, idx);
        self.bloom.insert(&id);
        self.inserts_since_refresh = self.inserts_since_refresh.saturating_add(1);

        if self.config.enable_learned_index {
            self.maybe_refresh();
        }

        id
    }

    pub fn put_with_delta(
        &mut self,
        kind: ValueKind,
        bytes: Vec<u8>,
        base: Option<ValueId>,
    ) -> ValueId {
        if !self.config.delta_policy.enabled || base.is_none() || kind != ValueKind::Cell {
            return self.put(kind, bytes);
        }

        let id = value_id(&bytes);
        if self.index.contains_key(&id) {
            return id;
        }

        let base_id = base.unwrap();
        let base_depth = match self.entry_by_id(&base_id) {
            Some(entry) => entry.delta.as_ref().map(|d| d.depth).unwrap_or(0),
            None => {
                return self.put(kind, bytes);
            }
        };

        let base_bytes = match self.materialize_internal(&base_id, false, false) {
            Ok((bytes, _)) => bytes,
            Err(_) => return self.put(kind, bytes),
        };

        let depth = base_depth.saturating_add(1);

        let full_len = bytes.len();
        let policy = self.config.delta_policy.clone();

        if full_len < policy.min_bytes {
            return self.put(kind, bytes);
        }

        if policy.max_chain > 0 && depth > policy.max_chain {
            return self.put(kind, bytes);
        }

        if policy.snapshot_interval > 0 && depth % policy.snapshot_interval == 0 {
            return self.put(kind, bytes);
        }

        let patch = build_patch_bytes(&base_bytes, &bytes);
        if patch.is_empty() {
            return self.put(kind, bytes);
        }

        let patch_len = patch.len();
        let min_allowed = (full_len as f64 * policy.min_savings_ratio) as usize;
        if patch_len > min_allowed {
            return self.put(kind, bytes);
        }

        let skip = if policy.max_skip > 0 {
            build_skip_patches(self, &bytes, base_id, depth, policy.max_skip)
        } else {
            Vec::new()
        };

        let entry = ValueEntry {
            kind: ValueKind::Delta,
            bytes: patch,
            delta: Some(DeltaEntry {
                base: base_id,
                full_len,
                depth,
                skip,
            }),
        };

        let idx = self.entries.len();
        self.entry_ids.push(id);
        self.entries.push(entry);
        self.index.insert(id, idx);
        self.bloom.insert(&id);
        self.inserts_since_refresh = self.inserts_since_refresh.saturating_add(1);

        self.delta_stats.values_written = self.delta_stats.values_written.saturating_add(1);
        self.delta_stats.bytes_saved = self
            .delta_stats
            .bytes_saved
            .saturating_add(full_len.saturating_sub(patch_len) as u64);
        self.delta_stats.total_depth = self.delta_stats.total_depth.saturating_add(depth as u64);

        if self.config.enable_learned_index {
            self.maybe_refresh();
        }

        id
    }

    pub fn materialize(&mut self, id: &ValueId) -> Result<Vec<u8>, ValueStoreError> {
        let (bytes, _trace) = self.materialize_internal(id, true, true)?;
        Ok(bytes)
    }

    pub fn materialize_with_trace(
        &mut self,
        id: &ValueId,
    ) -> Result<(Vec<u8>, DeltaReconstructTrace), ValueStoreError> {
        let (bytes, trace) = self.materialize_internal(id, true, true)?;
        Ok((bytes, trace))
    }

    pub fn compact_deltas(&mut self) -> DeltaCompactionReport {
        let policy = self.config.delta_policy.clone();
        let entries: Vec<(ValueId, usize)> =
            self.index.iter().map(|(id, idx)| (*id, *idx)).collect();

        let mut rewrite_ids = Vec::new();
        for (id, idx) in entries.iter() {
            let entry = match self.entries.get(*idx) {
                Some(entry) => entry,
                None => continue,
            };
            if entry.kind != ValueKind::Delta {
                continue;
            }
            let Some(delta) = entry.delta.as_ref() else {
                continue;
            };
            if policy.max_chain > 0 && delta.depth > policy.max_chain {
                rewrite_ids.push(*id);
                continue;
            }
            if policy.snapshot_interval > 0 && delta.depth % policy.snapshot_interval == 0 {
                rewrite_ids.push(*id);
            }
        }

        let mut rewritten = 0usize;
        let mut cpu_units = 0u64;
        let mut io_bytes_read = 0u64;
        let mut io_bytes_written = 0u64;
        for id in rewrite_ids.iter() {
            let chain_bytes = self.delta_chain_bytes(id).unwrap_or(0) as u64;
            if let Ok((bytes, _)) = self.materialize_internal(id, false, false) {
                if let Some(idx) = self.index.get(id).copied() {
                    if let Some(entry) = self.entries.get_mut(idx) {
                        entry.kind = ValueKind::Cell;
                        entry.bytes = bytes;
                        entry.delta = None;
                        rewritten = rewritten.saturating_add(1);
                        let written = entry.bytes.len() as u64;
                        io_bytes_read = io_bytes_read.saturating_add(chain_bytes);
                        io_bytes_written = io_bytes_written.saturating_add(written);
                        cpu_units = cpu_units.saturating_add(chain_bytes.saturating_add(written));
                    }
                }
            }
        }

        let mut skip_rebuilt = 0usize;
        if policy.max_skip > 0 {
            let entries: Vec<(ValueId, usize)> =
                self.index.iter().map(|(id, idx)| (*id, *idx)).collect();
            let mut updates: Vec<(ValueId, usize, Vec<SkipPatch>)> = Vec::new();

            for (id, idx) in entries.iter() {
                let base = {
                    let entry = match self.entries.get(*idx) {
                        Some(entry) => entry,
                        None => continue,
                    };
                    if entry.kind != ValueKind::Delta {
                        continue;
                    }
                    let Some(delta) = entry.delta.as_ref() else {
                        continue;
                    };
                    delta.base
                };
                let depth = match self.delta_chain_depth(id) {
                    Some(depth) => depth,
                    None => continue,
                };
                let bytes = match self.materialize_internal(id, false, false) {
                    Ok((bytes, _)) => bytes,
                    Err(_) => continue,
                };
                let skip = build_skip_patches(self, &bytes, base, depth, policy.max_skip);
                let skip_bytes: u64 = skip.iter().map(|s| s.patch.len() as u64).sum();
                updates.push((*id, depth, skip));

                let chain_bytes = self.delta_chain_bytes(id).unwrap_or(0) as u64;
                io_bytes_read = io_bytes_read.saturating_add(chain_bytes);
                io_bytes_written = io_bytes_written.saturating_add(skip_bytes);
                cpu_units = cpu_units
                    .saturating_add(chain_bytes)
                    .saturating_add(skip_bytes);
            }

            for (id, depth, skip) in updates.into_iter() {
                if let Some(idx) = self.index.get(&id).copied() {
                    if let Some(entry) = self.entries.get_mut(idx) {
                        if let Some(delta) = entry.delta.as_mut() {
                            delta.depth = depth;
                            delta.skip = skip;
                            skip_rebuilt = skip_rebuilt.saturating_add(1);
                        }
                    }
                }
            }
        }

        let energy = EnergyEstimate::from_counts(cpu_units, io_bytes_read, io_bytes_written);
        DeltaCompactionReport {
            rewritten,
            skip_rebuilt,
            energy,
        }
    }

    pub fn delta_benchmark(&mut self, ids: &[ValueId]) -> DeltaBenchmark {
        let mut steps = Vec::with_capacity(ids.len());
        let mut total = 0usize;
        for id in ids.iter() {
            if let Ok((_bytes, trace)) = self.materialize_internal(id, true, false) {
                steps.push(trace.steps);
                total = total.saturating_add(trace.steps);
            }
        }
        steps.sort_unstable();
        let p50 = quantile(&steps, 0.50);
        let p99 = quantile(&steps, 0.99);
        let p999 = quantile(&steps, 0.999);
        DeltaBenchmark {
            values: steps.len(),
            p50_steps: p50,
            p99_steps: p99,
            p999_steps: p999,
            avg_steps: if steps.is_empty() {
                0.0
            } else {
                total as f64 / steps.len() as f64
            },
        }
    }

    /// Analyze the delta chain topology for optimization opportunities.
    ///
    /// Returns statistics about chain depths, fan-out, and identifies hot chains
    /// (chains with high depth that would benefit from compaction or skip patches).
    pub fn topology_analysis(&self) -> DeltaTopologyReport {
        let mut total_entries = 0usize;
        let mut cell_entries = 0usize;
        let mut delta_entries = 0usize;
        let mut depths: Vec<usize> = Vec::new();
        let mut base_fanout: HashMap<ValueId, usize> = HashMap::new();
        let mut hot_chains: Vec<HotChainInfo> = Vec::new();
        let mut total_delta_bytes = 0u64;
        let mut total_cell_bytes = 0u64;
        let mut skip_patch_count = 0usize;
        let max_chain_depth = self.config.delta_policy.max_chain;

        for (id, idx) in self.index.iter() {
            let Some(entry) = self.entries.get(*idx) else {
                continue;
            };
            total_entries += 1;

            match entry.kind {
                ValueKind::Cell => {
                    cell_entries += 1;
                    total_cell_bytes += entry.bytes.len() as u64;
                }
                ValueKind::Delta => {
                    delta_entries += 1;
                    total_delta_bytes += entry.bytes.len() as u64;
                    if let Some(delta) = &entry.delta {
                        depths.push(delta.depth);
                        *base_fanout.entry(delta.base).or_insert(0) += 1;
                        skip_patch_count += delta.skip.len();

                        // Identify hot chains (depth exceeds 75% of max, or no skip patches on deep chains).
                        let depth_threshold = if max_chain_depth > 0 {
                            (max_chain_depth * 3) / 4
                        } else {
                            3
                        };
                        if delta.depth > depth_threshold {
                            hot_chains.push(HotChainInfo {
                                id: *id,
                                depth: delta.depth,
                                skip_patches: delta.skip.len(),
                                bytes: entry.bytes.len(),
                                needs_compaction: max_chain_depth > 0
                                    && delta.depth >= max_chain_depth,
                                needs_skip_patch: delta.depth > 2 && delta.skip.is_empty(),
                            });
                        }
                    }
                }
                _ => {}
            }
        }

        depths.sort_unstable();
        let avg_depth = if depths.is_empty() {
            0.0
        } else {
            depths.iter().sum::<usize>() as f64 / depths.len() as f64
        };
        let max_depth = depths.last().copied().unwrap_or(0);
        let p50_depth = quantile(&depths, 0.50);
        let p99_depth = quantile(&depths, 0.99);

        // Find high-fanout bases (many deltas share the same base).
        let max_fanout = base_fanout.values().copied().max().unwrap_or(0);
        let avg_fanout = if base_fanout.is_empty() {
            0.0
        } else {
            base_fanout.values().sum::<usize>() as f64 / base_fanout.len() as f64
        };

        // Sort hot chains by depth (worst first).
        hot_chains.sort_by(|a, b| b.depth.cmp(&a.depth));
        hot_chains.truncate(20);

        let savings_ratio = if total_cell_bytes + total_delta_bytes > 0 {
            total_delta_bytes as f64 / (total_cell_bytes + total_delta_bytes) as f64
        } else {
            0.0
        };

        DeltaTopologyReport {
            total_entries,
            cell_entries,
            delta_entries,
            avg_depth,
            max_depth,
            p50_depth,
            p99_depth,
            max_fanout,
            avg_fanout,
            skip_patch_count,
            total_cell_bytes,
            total_delta_bytes,
            savings_ratio,
            hot_chains,
        }
    }

    pub fn delta_chain_depth(&self, id: &ValueId) -> Option<usize> {
        let mut current = *id;
        let mut depth = 0usize;
        loop {
            let entry = self.entry_by_id(&current)?;
            if entry.kind != ValueKind::Delta {
                return Some(depth);
            }
            let delta = entry.delta.as_ref()?;
            depth = depth.saturating_add(1);
            current = delta.base;
        }
    }

    fn delta_chain_bytes(&self, id: &ValueId) -> Option<usize> {
        let mut current = *id;
        let mut total = 0usize;
        loop {
            let entry = self.entry_by_id(&current)?;
            total = total.saturating_add(entry.bytes.len());
            if entry.kind != ValueKind::Delta {
                return Some(total);
            }
            let delta = entry.delta.as_ref()?;
            current = delta.base;
        }
    }

    pub fn get(&mut self, id: &ValueId) -> Option<&ValueEntry> {
        let (entry, _trace) = self.get_with_trace(id);
        entry
    }

    pub fn get_with_trace(&mut self, id: &ValueId) -> (Option<&ValueEntry>, LookupTrace) {
        self.lookup_hist.record(id);
        self.stats.lookups = self.stats.lookups.saturating_add(1);

        let mut probes = 0usize;
        let mut used_learned = false;
        if self.config.enable_learned_index {
            if let Some(model) = &self.learned {
                if let Some((start, end)) = model.predict_range(id) {
                    used_learned = true;
                    for idx in start..=end {
                        probes = probes.saturating_add(1);
                        if let Some(key) = self.sorted_keys.get(idx) {
                            if key == id {
                                let entry_idx = self.sorted_entries[idx];
                                self.stats.learned_hits = self.stats.learned_hits.saturating_add(1);
                                self.stats.probes = self.stats.probes.saturating_add(probes as u64);
                                return (
                                    self.entries.get(entry_idx),
                                    LookupTrace {
                                        probes,
                                        used_learned,
                                        used_fallback: false,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }

        let entry = self.index.get(id).and_then(|idx| self.entries.get(*idx));
        if entry.is_some() {
            probes = probes.saturating_add(1);
            self.stats.fallback_hits = self.stats.fallback_hits.saturating_add(1);
        }
        self.stats.probes = self.stats.probes.saturating_add(probes as u64);

        (
            entry,
            LookupTrace {
                probes,
                used_learned,
                used_fallback: entry.is_some(),
            },
        )
    }

    fn entry_by_id(&self, id: &ValueId) -> Option<&ValueEntry> {
        self.index.get(id).and_then(|idx| self.entries.get(*idx))
    }

    fn materialize_internal(
        &mut self,
        id: &ValueId,
        use_skip: bool,
        track_stats: bool,
    ) -> Result<(Vec<u8>, DeltaReconstructTrace), ValueStoreError> {
        let mut cache: HashMap<ValueId, (Vec<u8>, DeltaReconstructTrace)> = HashMap::new();
        let (bytes, trace) = self.materialize_with_cache(id, use_skip, &mut cache)?;

        if track_stats && trace.steps > 0 {
            self.delta_stats.reconstruct_calls =
                self.delta_stats.reconstruct_calls.saturating_add(1);
            self.delta_stats.reconstruct_steps = self
                .delta_stats
                .reconstruct_steps
                .saturating_add(trace.steps as u64);
        }

        Ok((bytes, trace))
    }

    fn materialize_with_cache(
        &mut self,
        id: &ValueId,
        use_skip: bool,
        cache: &mut HashMap<ValueId, (Vec<u8>, DeltaReconstructTrace)>,
    ) -> Result<(Vec<u8>, DeltaReconstructTrace), ValueStoreError> {
        if let Some(hit) = cache.get(id) {
            return Ok(hit.clone());
        }

        let entry = self.entry_by_id(id).ok_or(ValueStoreError::NotFound)?;
        let (bytes, trace) = if entry.kind != ValueKind::Delta {
            (
                entry.bytes.clone(),
                DeltaReconstructTrace {
                    steps: 0,
                    used_skip: false,
                },
            )
        } else {
            let delta = entry.delta.as_ref().ok_or(ValueStoreError::InvalidDelta)?;
            let base_id = delta.base;
            let full_len = delta.full_len;
            let mut candidates: Vec<(ValueId, Vec<u8>, bool)> = Vec::new();
            candidates.push((base_id, entry.bytes.clone(), false));
            if use_skip {
                for skip in delta.skip.iter() {
                    candidates.push((skip.base, skip.patch.clone(), true));
                }
            }

            let mut best: Option<(Vec<u8>, DeltaReconstructTrace)> = None;
            for (base_id, patch, is_skip) in candidates.into_iter() {
                let (base_bytes, base_trace) =
                    self.materialize_with_cache(&base_id, use_skip, cache)?;
                let value = apply_patch_bytes(&base_bytes, &patch)?;
                if value.len() != full_len {
                    return Err(ValueStoreError::DeltaLengthMismatch);
                }
                let trace = DeltaReconstructTrace {
                    steps: base_trace.steps.saturating_add(1),
                    used_skip: base_trace.used_skip || is_skip,
                };
                if best.as_ref().map(|(_, t)| t.steps).unwrap_or(usize::MAX) > trace.steps {
                    best = Some((value, trace));
                }
            }
            let Some((value, trace)) = best else {
                return Err(ValueStoreError::InvalidDelta);
            };
            if value_id(&value) != *id {
                return Err(ValueStoreError::InvalidDelta);
            }
            (value, trace)
        };

        cache.insert(*id, (bytes.clone(), trace.clone()));
        Ok((bytes, trace))
    }

    pub fn should_refresh(&self) -> bool {
        if !self.config.enable_learned_index {
            return false;
        }
        if self.entries.len() < self.config.refresh_policy.min_samples {
            return false;
        }
        if self.learned.is_none() {
            return true;
        }
        if self.inserts_since_refresh >= self.config.refresh_policy.max_inserts {
            return true;
        }
        if let Some(model_hist) = &self.model_hist {
            let shift = self.lookup_hist.l1_distance(model_hist);
            if shift >= self.config.refresh_policy.max_shift_score {
                return true;
            }
        }
        false
    }

    pub fn maybe_refresh(&mut self) -> bool {
        if self.should_refresh() {
            self.refresh_learned_index();
            return true;
        }
        false
    }

    pub fn refresh_learned_index(&mut self) {
        if !self.config.enable_learned_index {
            self.learned = None;
            self.sorted_keys.clear();
            self.sorted_entries.clear();
            return;
        }

        self.rebuild_sorted_keys();
        let segment_size = self.config.segment_size.max(16);
        if self.sorted_keys.len() >= self.config.refresh_policy.min_samples {
            self.learned = Some(LearnedIndex::build(&self.sorted_keys, segment_size));
        } else {
            self.learned = None;
        }
        self.inserts_since_refresh = 0;
        self.model_hist = Some(self.lookup_hist.clone());
    }

    pub fn benchmark(&mut self, keys: &[ValueId]) -> LookupBenchmark {
        let mut probes = Vec::with_capacity(keys.len());
        let mut learned_hits = 0u64;

        for id in keys {
            let (_entry, trace) = self.get_with_trace(id);
            probes.push(trace.probes);
            if trace.used_learned {
                learned_hits = learned_hits.saturating_add(1);
            }
        }

        probes.sort_unstable();
        let p50 = quantile(&probes, 0.50);
        let p99 = quantile(&probes, 0.99);
        let p999 = quantile(&probes, 0.999);

        LookupBenchmark {
            lookups: keys.len(),
            p50_probes: p50,
            p99_probes: p99,
            p999_probes: p999,
            memory_bytes: self.approx_memory_bytes(),
            learned_hit_rate: if keys.is_empty() {
                0.0
            } else {
                learned_hits as f64 / keys.len() as f64
            },
        }
    }

    pub fn approx_memory_bytes(&self) -> usize {
        let entry_bytes: usize = self.entries.iter().map(|e| e.bytes.len()).sum();
        let skip_bytes: usize = self
            .entries
            .iter()
            .filter_map(|e| e.delta.as_ref())
            .map(|d| d.skip.iter().map(|s| s.patch.len()).sum::<usize>())
            .sum();
        let id_bytes = self.entry_ids.len() * std::mem::size_of::<ValueId>();
        let index_bytes =
            self.index.len() * (std::mem::size_of::<ValueId>() + std::mem::size_of::<usize>());
        let sorted_bytes = self.sorted_keys.len() * std::mem::size_of::<ValueId>()
            + self.sorted_entries.len() * std::mem::size_of::<usize>();
        let model_bytes = self.learned.as_ref().map(|m| m.approx_bytes()).unwrap_or(0);
        entry_bytes + skip_bytes + id_bytes + index_bytes + sorted_bytes + model_bytes
    }

    pub fn write_segment_file(&self, path: impl AsRef<Path>) -> Result<(), ValueStoreError> {
        let mut writer = ValueSegmentWriter::create(path)?;
        for (id, entry) in self.entry_ids.iter().copied().zip(self.entries.iter()) {
            writer.append(id, entry)?;
        }
        writer.sync()?;
        Ok(())
    }

    pub fn load_segment_file(
        path: impl AsRef<Path>,
        config: ValueStoreConfig,
    ) -> Result<Self, ValueStoreError> {
        ValueSegmentReader::open(path)?.load_store(config)
    }

    pub fn export_transfer_entry(&mut self, id: &ValueId) -> Result<Vec<u8>, ValueStoreError> {
        let entry = self.get(id).cloned().ok_or(ValueStoreError::NotFound)?;
        encode_vseg_entry(*id, &entry)
    }

    pub fn import_transfer_entries<I>(&mut self, entries: I) -> Result<usize, ValueStoreError>
    where
        I: IntoIterator<Item = ValueSegmentEntry>,
    {
        let mut inserted = 0usize;
        for entry in entries {
            if self.index.contains_key(&entry.id) {
                continue;
            }
            self.insert_loaded_entry(entry.id, entry.entry)?;
            inserted = inserted.saturating_add(1);
        }
        if inserted > 0 {
            self.recompute_loaded_delta_metadata()?;
        }
        Ok(inserted)
    }

    fn insert_loaded_entry(
        &mut self,
        id: ValueId,
        entry: ValueEntry,
    ) -> Result<(), ValueStoreError> {
        if self.index.contains_key(&id) {
            return Err(ValueStoreError::DuplicateValueId);
        }
        let idx = self.entries.len();
        self.entry_ids.push(id);
        self.entries.push(entry);
        self.index.insert(id, idx);
        self.bloom.insert(&id);
        Ok(())
    }

    fn recompute_loaded_delta_metadata(&mut self) -> Result<(), ValueStoreError> {
        let ids = self.entry_ids.clone();
        let mut memo = HashMap::new();
        for id in ids.iter().copied() {
            let depth = self.rebuild_loaded_delta_depth(id, &mut memo, &mut HashSet::new())?;
            if let Some(idx) = self.index.get(&id).copied() {
                if let Some(delta) = self.entries[idx].delta.as_mut() {
                    delta.depth = depth;
                    delta.skip.clear();
                }
            }
        }

        self.delta_stats = DeltaStats::default();
        for id in ids.iter() {
            if let Some(idx) = self.index.get(id).copied() {
                let entry = &self.entries[idx];
                if entry.kind == ValueKind::Delta {
                    let delta = entry.delta.as_ref().ok_or(ValueStoreError::InvalidDelta)?;
                    self.delta_stats.values_written =
                        self.delta_stats.values_written.saturating_add(1);
                    self.delta_stats.bytes_saved = self
                        .delta_stats
                        .bytes_saved
                        .saturating_add(delta.full_len.saturating_sub(entry.bytes.len()) as u64);
                    self.delta_stats.total_depth = self
                        .delta_stats
                        .total_depth
                        .saturating_add(delta.depth as u64);
                    let _ = self.materialize_internal(id, false, false)?;
                }
            }
        }

        if self.config.enable_learned_index {
            self.refresh_learned_index();
        }

        Ok(())
    }

    fn rebuild_loaded_delta_depth(
        &self,
        id: ValueId,
        memo: &mut HashMap<ValueId, usize>,
        visiting: &mut HashSet<ValueId>,
    ) -> Result<usize, ValueStoreError> {
        if let Some(depth) = memo.get(&id).copied() {
            return Ok(depth);
        }
        if !visiting.insert(id) {
            return Err(ValueStoreError::InvalidDelta);
        }

        let depth = match self.entry_by_id(&id) {
            Some(entry) if entry.kind == ValueKind::Delta => {
                let delta = entry.delta.as_ref().ok_or(ValueStoreError::InvalidDelta)?;
                if self.entry_by_id(&delta.base).is_none() {
                    return Err(ValueStoreError::MissingDeltaBase);
                }
                self.rebuild_loaded_delta_depth(delta.base, memo, visiting)?
                    .saturating_add(1)
            }
            Some(_) => 0,
            None => return Err(ValueStoreError::NotFound),
        };

        visiting.remove(&id);
        memo.insert(id, depth);
        Ok(depth)
    }

    fn rebuild_sorted_keys(&mut self) {
        let mut pairs: Vec<(ValueId, usize)> =
            self.index.iter().map(|(id, idx)| (*id, *idx)).collect();
        pairs.sort_by_key(|(id, _)| id_to_u128(id));
        self.sorted_keys = pairs.iter().map(|(id, _)| *id).collect();
        self.sorted_entries = pairs.iter().map(|(_, idx)| *idx).collect();
    }
}

#[derive(Debug, Clone)]
struct LearnedIndex {
    segments: Vec<LearnedSegment>,
    total_keys: usize,
}

impl LearnedIndex {
    fn build(keys: &[ValueId], segment_size: usize) -> Self {
        let mut segments = Vec::new();
        let total = keys.len();
        if total == 0 {
            return Self {
                segments,
                total_keys: 0,
            };
        }

        let mut start = 0usize;
        while start < total {
            let end = (start + segment_size).min(total) - 1;
            let segment = LearnedSegment::build(keys, start, end);
            segments.push(segment);
            start = end + 1;
        }

        Self {
            segments,
            total_keys: total,
        }
    }

    fn predict_range(&self, id: &ValueId) -> Option<(usize, usize)> {
        if self.total_keys == 0 {
            return None;
        }
        let key = id_to_u64_prefix(id);
        let seg_idx = self
            .segments
            .binary_search_by_key(&key, |seg| seg.end_key)
            .unwrap_or_else(|idx| idx);
        let seg = self.segments.get(seg_idx)?;

        let pred = seg.predict(key);
        let max_err = seg.max_error.saturating_add(1);
        let start = pred.saturating_sub(max_err);
        let end = (pred + max_err).min(self.total_keys.saturating_sub(1));
        Some((start, end))
    }

    fn approx_bytes(&self) -> usize {
        self.segments.len() * std::mem::size_of::<LearnedSegment>()
    }
}

#[derive(Debug, Clone)]
struct LearnedSegment {
    end_key: u64,
    slope: f64,
    intercept: f64,
    max_error: usize,
}

impl LearnedSegment {
    fn build(keys: &[ValueId], start: usize, end: usize) -> Self {
        let start_key = id_to_u64_prefix(&keys[start]);
        let end_key = id_to_u64_prefix(&keys[end]);
        let start_pos = start as f64;
        let end_pos = end as f64;
        let key_span = (end_key as i128 - start_key as i128) as f64;
        let slope = if key_span.abs() < f64::EPSILON {
            0.0
        } else {
            (end_pos - start_pos) / key_span
        };
        let intercept = start_pos - slope * start_key as f64;

        let mut max_error = 0usize;
        for (idx, key) in keys[start..=end].iter().enumerate() {
            let pos = start + idx;
            let pred = slope * id_to_u64_prefix(key) as f64 + intercept;
            let err = (pred - pos as f64).abs().round() as usize;
            max_error = max_error.max(err);
        }

        Self {
            end_key,
            slope,
            intercept,
            max_error,
        }
    }

    fn predict(&self, key: u64) -> usize {
        let pred = self.slope * key as f64 + self.intercept;
        if pred.is_nan() || pred.is_infinite() {
            return 0;
        }
        pred.round().max(0.0) as usize
    }
}

fn id_to_u128(id: &ValueId) -> u128 {
    let mut v = 0u128;
    for b in id.iter() {
        v = (v << 8) | *b as u128;
    }
    v
}

fn id_to_u64_prefix(id: &ValueId) -> u64 {
    let mut v = 0u64;
    for b in id[0..8].iter() {
        v = (v << 8) | *b as u64;
    }
    v
}

fn quantile(xs: &[usize], q: f64) -> usize {
    if xs.is_empty() {
        return 0;
    }
    let idx = ((xs.len() - 1) as f64 * q).round() as usize;
    xs[idx.min(xs.len() - 1)]
}

fn build_patch_bytes(base: &[u8], target: &[u8]) -> Vec<u8> {
    if base == target {
        return Vec::new();
    }

    let prefix = common_prefix_len(base, target);
    let suffix = common_suffix_len(base, target, prefix);

    let mut ops: Vec<Vec<u8>> = Vec::new();
    if prefix > 0 {
        let mut op = Vec::new();
        op.push(0);
        op.extend_from_slice(&encode_varu(0));
        op.extend_from_slice(&encode_varu(prefix as u64));
        ops.push(op);
    }

    let mid_start = prefix;
    let mid_end = target.len().saturating_sub(suffix);
    if mid_end > mid_start {
        let mut op = Vec::new();
        op.push(1);
        let slice = &target[mid_start..mid_end];
        op.extend_from_slice(&encode_varu(slice.len() as u64));
        op.extend_from_slice(slice);
        ops.push(op);
    }

    if suffix > 0 {
        let mut op = Vec::new();
        op.push(0);
        let base_off = base.len().saturating_sub(suffix);
        op.extend_from_slice(&encode_varu(base_off as u64));
        op.extend_from_slice(&encode_varu(suffix as u64));
        ops.push(op);
    }

    let mut out = Vec::new();
    out.extend_from_slice(&encode_varu(target.len() as u64));
    out.extend_from_slice(&encode_varu(ops.len() as u64));
    for op in ops {
        out.extend_from_slice(&op);
    }
    out
}

fn apply_patch_bytes(base: &[u8], patch: &[u8]) -> Result<Vec<u8>, ValueStoreError> {
    let mut cursor = patch;
    let target_len = decode_varu(&mut cursor)? as usize;
    let op_count = decode_varu(&mut cursor)? as usize;

    let mut out = Vec::with_capacity(target_len);
    for _ in 0..op_count {
        let Some((&tag, rest)) = cursor.split_first() else {
            return Err(ValueStoreError::InvalidDelta);
        };
        cursor = rest;
        match tag {
            0 => {
                let off = decode_varu(&mut cursor)? as usize;
                let len = decode_varu(&mut cursor)? as usize;
                let end = off.saturating_add(len);
                if end > base.len() {
                    return Err(ValueStoreError::InvalidDelta);
                }
                out.extend_from_slice(&base[off..end]);
            }
            1 => {
                let len = decode_varu(&mut cursor)? as usize;
                if cursor.len() < len {
                    return Err(ValueStoreError::InvalidDelta);
                }
                out.extend_from_slice(&cursor[..len]);
                cursor = &cursor[len..];
            }
            _ => return Err(ValueStoreError::InvalidDelta),
        }
    }

    if !cursor.is_empty() {
        return Err(ValueStoreError::InvalidDelta);
    }
    if out.len() != target_len {
        return Err(ValueStoreError::DeltaLengthMismatch);
    }
    Ok(out)
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let max = a.len().min(b.len());
    let mut i = 0usize;
    while i < max && a[i] == b[i] {
        i += 1;
    }
    i
}

fn common_suffix_len(a: &[u8], b: &[u8], prefix: usize) -> usize {
    let max = a.len().min(b.len());
    let mut i = 0usize;
    while i + prefix < max && a[a.len().saturating_sub(1 + i)] == b[b.len().saturating_sub(1 + i)] {
        i += 1;
    }
    i
}

fn build_skip_patches(
    store: &mut ValueStore,
    bytes: &[u8],
    base_id: ValueId,
    depth: usize,
    max_skip: usize,
) -> Vec<SkipPatch> {
    let mut skip = Vec::new();
    let mut distance = 2usize;

    while distance <= depth && skip.len() < max_skip {
        let ancestor = match ancestor_at_distance(store, base_id, distance.saturating_sub(1)) {
            Some(id) => id,
            None => break,
        };
        let base_bytes = match store.materialize_internal(&ancestor, false, false) {
            Ok((bytes, _)) => bytes,
            Err(_) => break,
        };
        let patch = build_patch_bytes(&base_bytes, bytes);
        if patch.is_empty() {
            break;
        }
        skip.push(SkipPatch {
            base: ancestor,
            patch,
            distance,
        });
        distance = distance.saturating_mul(2);
    }

    skip
}

pub struct ValueSegmentWriter {
    path: PathBuf,
    file: File,
}

impl ValueSegmentWriter {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, ValueStoreError> {
        Self::open_impl(path, true)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, ValueStoreError> {
        Self::open_impl(path, false)
    }

    fn open_impl(path: impl AsRef<Path>, truncate_existing: bool) -> Result<Self, ValueStoreError> {
        let path = path.as_ref().to_path_buf();
        let existed = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(truncate_existing)
            .open(&path)?;

        if !existed || truncate_existing || file.metadata()?.len() == 0 {
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&FileHeader::new(FileKind::ValSeg, 0, now_unix_s()).encode())?;
            file.flush()?;
        } else {
            validate_vseg_header(&mut file)?;
        }

        file.seek(SeekFrom::End(0))?;
        Ok(Self { path, file })
    }

    pub fn append(&mut self, id: ValueId, entry: &ValueEntry) -> Result<(), ValueStoreError> {
        let payload = encode_vseg_entry(id, entry)?;
        let mut frame = Vec::with_capacity(payload.len() + 8);
        append_record_frame(&mut frame, &payload)?;
        self.file.write_all(&frame)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn sync(&self) -> Result<(), ValueStoreError> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct ValueSegmentReader {
    path: PathBuf,
}

impl ValueSegmentReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ValueStoreError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).open(&path)?;
        validate_vseg_header(&mut file)?;
        Ok(Self { path })
    }

    pub fn read_all(&self) -> Result<Vec<ValueSegmentEntry>, ValueStoreError> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        file.seek(SeekFrom::Start(FILE_HEADER_LEN as u64))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut rest = bytes.as_slice();
        let mut entries = Vec::new();
        while !rest.is_empty() {
            let payload = decode_record_frame(&mut rest)?;
            entries.push(decode_vseg_entry(&payload)?);
        }
        Ok(entries)
    }

    pub fn load_store(&self, config: ValueStoreConfig) -> Result<ValueStore, ValueStoreError> {
        let entries = self.read_all()?;
        let mut store = ValueStore::new(config);
        for entry in entries {
            store.insert_loaded_entry(entry.id, entry.entry)?;
        }
        store.recompute_loaded_delta_metadata()?;
        Ok(store)
    }
}

pub fn encode_transfer_entry(id: ValueId, entry: &ValueEntry) -> Result<Vec<u8>, ValueStoreError> {
    encode_vseg_entry(id, entry)
}

pub fn decode_transfer_entry(payload: &[u8]) -> Result<ValueSegmentEntry, ValueStoreError> {
    decode_vseg_entry(payload)
}

pub fn materialize_transfer_entry_with_resolver<F>(
    id: &ValueId,
    entry: &ValueEntry,
    base_resolver: &mut F,
) -> Result<Vec<u8>, ValueStoreError>
where
    F: FnMut(&ValueId) -> Result<Vec<u8>, ValueStoreError>,
{
    if entry.kind != ValueKind::Delta {
        let bytes = entry.bytes.clone();
        if value_id(&bytes) != *id {
            return Err(ValueStoreError::InvalidSegmentEntry(
                "transfer value id does not match raw bytes",
            ));
        }
        return Ok(bytes);
    }

    let delta = entry.delta.as_ref().ok_or(ValueStoreError::InvalidDelta)?;
    let base_bytes = base_resolver(&delta.base)?;
    let value = apply_patch_bytes(&base_bytes, &entry.bytes)?;
    if value.len() != delta.full_len {
        return Err(ValueStoreError::DeltaLengthMismatch);
    }
    if value_id(&value) != *id {
        return Err(ValueStoreError::InvalidDelta);
    }
    Ok(value)
}

fn validate_vseg_header(file: &mut File) -> Result<(), ValueStoreError> {
    let len = file.metadata()?.len();
    if len < FILE_HEADER_LEN as u64 {
        return Err(ValueStoreError::BadHeader);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; FILE_HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|_| ValueStoreError::BadHeader)?;
    let header = FileHeader::decode(&header)?;
    if header.file_kind != FileKind::ValSeg {
        return Err(ValueStoreError::BadHeader);
    }
    Ok(())
}

fn encode_vseg_entry(id: ValueId, entry: &ValueEntry) -> Result<Vec<u8>, ValueStoreError> {
    let mut out = vec![
        VSEG_RECORD_TYPE,
        VSEG_RECORD_VERSION,
        entry.kind as u8,
        ValueSegmentCodec::Raw as u8,
    ];
    out.extend_from_slice(&id);

    let (raw_len, stored_bytes) = match entry.kind {
        ValueKind::Delta => {
            let delta = entry.delta.as_ref().ok_or(ValueStoreError::InvalidDelta)?;
            (delta.full_len, encode_delta1_bytes(delta, &entry.bytes)?)
        }
        _ => (entry.bytes.len(), entry.bytes.clone()),
    };

    out.extend_from_slice(&encode_varu(raw_len as u64));
    out.extend_from_slice(&encode_varu(stored_bytes.len() as u64));
    out.extend_from_slice(&stored_bytes);
    Ok(out)
}

fn decode_vseg_entry(payload: &[u8]) -> Result<ValueSegmentEntry, ValueStoreError> {
    if payload.len() < 20 {
        return Err(ValueStoreError::Format(FormatError::UnexpectedEof));
    }
    if payload[0] != VSEG_RECORD_TYPE {
        return Err(ValueStoreError::InvalidSegmentRecordType(payload[0]));
    }
    if payload[1] != VSEG_RECORD_VERSION {
        return Err(ValueStoreError::UnsupportedSegmentRecordVersion(payload[1]));
    }

    let kind = value_kind_from_u8(payload[2])?;
    let codec = ValueSegmentCodec::from_u8(payload[3])?;
    if codec != ValueSegmentCodec::Raw {
        return Err(ValueStoreError::UnsupportedValueCodec(payload[3]));
    }

    let mut id = [0u8; 16];
    id.copy_from_slice(&payload[4..20]);

    let mut rest = &payload[20..];
    let raw_len = decode_varu(&mut rest)? as usize;
    let stored_len = decode_varu(&mut rest)? as usize;
    if rest.len() < stored_len {
        return Err(ValueStoreError::Format(FormatError::UnexpectedEof));
    }
    let stored_bytes = rest[..stored_len].to_vec();
    rest = &rest[stored_len..];
    if !rest.is_empty() {
        return Err(ValueStoreError::InvalidSegmentEntry(
            "trailing bytes after VE1 payload",
        ));
    }

    let entry = match kind {
        ValueKind::Delta => {
            let (delta, patch_bytes) = decode_delta1_bytes(raw_len, &stored_bytes)?;
            ValueEntry {
                kind,
                bytes: patch_bytes,
                delta: Some(delta),
            }
        }
        _ => {
            if raw_len != stored_bytes.len() {
                return Err(ValueStoreError::InvalidSegmentEntry(
                    "raw_len does not match stored raw bytes",
                ));
            }
            ValueEntry {
                kind,
                bytes: stored_bytes,
                delta: None,
            }
        }
    };

    Ok(ValueSegmentEntry { id, entry })
}

fn encode_delta1_bytes(delta: &DeltaEntry, patch_bytes: &[u8]) -> Result<Vec<u8>, ValueStoreError> {
    let mut out = Vec::new();
    out.extend_from_slice(&delta.base);
    out.push(DELTA1_CODEC_COPY_ADD);
    out.extend_from_slice(&encode_varu(delta.full_len as u64));
    out.extend_from_slice(&encode_varu(patch_bytes.len() as u64));
    out.extend_from_slice(patch_bytes);
    Ok(out)
}

fn decode_delta1_bytes(
    raw_len: usize,
    stored_bytes: &[u8],
) -> Result<(DeltaEntry, Vec<u8>), ValueStoreError> {
    if stored_bytes.len() < 17 {
        return Err(ValueStoreError::Format(FormatError::UnexpectedEof));
    }
    let mut base = [0u8; 16];
    base.copy_from_slice(&stored_bytes[..16]);
    let delta_codec = stored_bytes[16];
    if delta_codec != DELTA1_CODEC_COPY_ADD {
        return Err(ValueStoreError::UnsupportedDeltaCodec(delta_codec));
    }
    let mut rest = &stored_bytes[17..];
    let full_len = decode_varu(&mut rest)? as usize;
    if full_len != raw_len {
        return Err(ValueStoreError::InvalidSegmentEntry(
            "delta full_len does not match VE1 raw_len",
        ));
    }
    let patch_len = decode_varu(&mut rest)? as usize;
    if rest.len() < patch_len {
        return Err(ValueStoreError::Format(FormatError::UnexpectedEof));
    }
    let patch_bytes = rest[..patch_len].to_vec();
    rest = &rest[patch_len..];
    if !rest.is_empty() {
        return Err(ValueStoreError::InvalidSegmentEntry(
            "trailing bytes after DELTA1 payload",
        ));
    }

    Ok((
        DeltaEntry {
            base,
            full_len,
            depth: 0,
            skip: Vec::new(),
        },
        patch_bytes,
    ))
}

fn value_kind_from_u8(v: u8) -> Result<ValueKind, ValueStoreError> {
    match v {
        1 => Ok(ValueKind::Cell),
        2 => Ok(ValueKind::Group),
        3 => Ok(ValueKind::BlobChunk),
        4 => Ok(ValueKind::BlobManifest),
        5 => Ok(ValueKind::Delta),
        6 => Ok(ValueKind::Embedding),
        _ => Err(ValueStoreError::InvalidValueKind(v)),
    }
}

fn now_unix_s() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn ancestor_at_distance(
    store: &ValueStore,
    mut current: ValueId,
    mut steps: usize,
) -> Option<ValueId> {
    while steps > 0 {
        let entry = store.entry_by_id(&current)?;
        let delta = entry.delta.as_ref()?;
        current = delta.base;
        steps = steps.saturating_sub(1);
    }
    Some(current)
}
