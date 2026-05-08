# Delta-chained MVCC Values (Design)

Status: Hardened research baseline
Last updated: 2026-05-08

Goal:
Reduce write amplification when large values (TEXT/JSON/BLOB) change slightly across MVCC versions.

SkeinDB already supports value interning (content-addressed ValueStore). Delta values extend this by allowing a ValueStore entry to be stored as a patch against a base value, rather than as a full copy.

Current implementation note: R03 is now hardened with periodic full snapshots (`DeltaPolicy.snapshot_interval`), geometric skip patches, compaction-time delta rewrites / skip rebuilds, `DeltaCompactionReport` including energy estimates, `ValueStore::delta_benchmark()` p50/p99/p99.9 reconstruction-step reporting, and `ValueStore::topology_analysis()` depth/fanout/savings reports.

This feature is intended to be optional and policy-controlled.

---

## 1) High-level idea

In classic MVCC, an UPDATE creates a new version of a row. If a large column changes (even slightly), the new version may store the entire new value.

Delta values store:
- base ValueID (the previous value)
- a patch that transforms base -> new
- metadata needed for safe reconstruction and validation

The ValueID remains the content-hash of the FULL reconstructed value. This preserves dedup semantics and keeps external references stable.

---

## 2) ValueStore format extension

Add a new ValueEntry kind:
- val_kind = DELTA

Conceptual payload:

DELTA1:
- base_vid[16]
- delta_codec u8
- full_len VarU
- patch_bytes Bytes

Where:
- delta_codec identifies the patch encoding.
- patch_bytes are typically compressed.

ValueID rules:
- value_id is computed over the reconstructed bytes (full value), not patch_bytes.

Safety rules:
- On read, reconstruct bytes then verify hash matches value_id.

---

## 3) Patch encoding (v1 recommendation)

Use a simple instruction stream similar to COPY/ADD patches:

Patch:
- target_len VarU
- op_count VarU
- repeated ops:
  - op_tag u8
    - 0 = COPY (from base)
      - base_off VarU
      - len VarU
    - 1 = ADD (literal)
      - bytes Bytes

This patch format is:
- deterministic
- easy to validate (bounds checks)
- simple to implement

Future codecs:
- a more compact delta (VCDIFF-like)
- specialized JSON diff (structural)

---

## 4) Delta selection policy

Delta creation should be selective.

Recommended policy knobs:
- delta_min_bytes: only consider DELTA if full_len >= threshold
- delta_max_chain: maximum allowed delta depth (default 4)
- delta_min_savings_ratio: require patch_len <= full_len * ratio (e.g., 0.7)
- delta_columns: allowlist of columns eligible for deltas

When a new value is written:
1) If prior value exists and column is eligible, compute patch.
2) If patch is small enough, store DELTA entry keyed by ValueID(full).
3) Else store RAW entry.

---

## 5) Reading DELTA values

To materialize a ValueID:
- Load ValueEntry by ValueID.
- If RAW, return bytes.
- If DELTA:
  1) recursively materialize base_vid
  2) apply patch to base bytes
  3) validate full hash

Caching:
- cache reconstructed bytes for hot values
- cache base values to reduce recursion cost

---

## 6) Compaction and rebasing

Delta chains should be bounded.

During compaction:
- if a DELTA chain depth > delta_max_chain, rewrite the value as RAW (or as a shorter delta against a nearer base)

This trades storage for read latency stability.

---

## 7) Interaction with chunked BLOBs

Recommended split:
- Use DELTA for TEXT/JSON where edits are localized.
- Use chunked BLOBs for very large binary values.

Chunking and delta can coexist:
- A BLOB manifest may itself be delta-encoded (optional), but start simple.

---

## 8) Observability

Expose metrics:
- delta_values_written
- delta_bytes_saved_estimate
- avg_delta_chain_depth
- delta_read_reconstruct_time

These are critical for tuning.

---

## 9) Research extension: delta-chain topology optimization

The baseline design above uses **linear** delta chains with periodic rebasing.
The research agenda proposes exploring richer topologies to improve historical reads and long version chains.
See: `docs/research_agenda/R03_delta-chain-topology-optimization.md`.

Candidate extensions (experimental):
- **Periodic full snapshots:** store a RAW snapshot every K deltas (fixed or adaptive) to bound reconstruction cost.
- **Skip pointers (skip-list style):** allow DELTA entries to include optional “skip base” pointers at geometric intervals, enabling O(log n) reconstruction steps for deep histories.
- **Tree-structured deltas:** when branches occur (e.g., concurrent updates), allow multiple child deltas to reference shared snapshots.

Compaction interaction:
- Compaction can opportunistically restructure delta topology, amortizing reorganization costs.
- Policy may be **per-key** based on observed access patterns (latest-only vs frequent time-travel reads).

Shipped research baseline:
- ValueStore supports delta patches using COPY/ADD encoding (prefix/suffix diff).
- Delta policy knobs live in `ValueStoreConfig.delta_policy` (min_bytes, max_chain, snapshot_interval, min_savings_ratio, max_skip).
- Skip patches are stored at geometric distances (2, 4, 8, ...) when enabled.
- `ValueStore::compact_deltas` rewrites deep chains to RAW and rebuilds skip patches.
