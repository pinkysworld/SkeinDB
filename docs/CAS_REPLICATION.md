# CAS-aware Replication and Bandwidth Bounds

Status: Draft
Last updated: 2026-01-17

Goal:
Use SkeinDB's content-addressed ValueStore (ValueID) to reduce replication and rebalancing bandwidth.

Key idea:
- Replication streams row/version metadata that references ValueIDs.
- The receiver transfers only the value objects it does not already have.

This document complements docs/CLUSTERING.md and focuses on:
1) the on-wire protocol for missing-object retrieval, and
2) measurable bandwidth bounds/metrics that can be reported in research/evaluation.

---

## 1) Assumptions

- Values and groups are immutable objects stored in the ValueStore.
- Objects are addressed by ValueID = hash(content).
- Row versions reference ValueIDs (directly or indirectly).

Because objects are immutable and addressed by content, a receiver can safely:
- deduplicate across tables/shards/nodes,
- cache objects permanently until GC.

---

## 2) Baseline replication (WAL shipping)

Baseline Level-1 replication is WAL shipping:
- primary emits WAL records in LSN order
- replica applies committed txns

In SkeinDB WAL records for row versions may reference ValueIDs.

A naive replication design would inline full values in the stream.
CAS-aware replication instead splits the stream:

1) stream references (row versions, index updates, schema)
2) fetch missing objects on demand

---

## 3) Missing object detection

Replica must answer: "do I already have ValueID X?"

### 3.1 Direct check

- Lookup ValueID in valdir (ValueID -> FilePtr)
- If present, the object exists

This is correct but may be too slow if performed for every referenced ValueID in a hot stream.

### 3.2 Bloom summaries (recommended)

Maintain per-segment summaries:
- for each valseg file, build a Bloom filter over ValueIDs contained in that segment
- keep a union Bloom for all live segments

On replicate/apply:
- check Bloom first
- if Bloom says "not present" -> definitely missing
- if Bloom says "maybe" -> do valdir lookup to confirm

This reduces random lookups when most values are missing or most are present.

---

## 4) Missing object pull protocol

### 4.1 Two-channel replication

Channel A: WAL/metadata stream
- carries row versions and references (ValueIDs)

Channel B: object fetch
- request/response to obtain object bytes by ValueID

### 4.2 Object fetch RPCs (conceptual)

- `objects.need` (primary -> replica): advertise a batch of ValueIDs referenced by recent WAL
- `objects.missing` (replica -> primary): return the subset that is missing
- `objects.fetch` (primary -> replica): stream VE entries (ValueID + bytes)

An alternative is replica-initiated pull:
- replica requests missing ValueIDs directly when apply fails due to missing objects

Batching is recommended to amortize overhead.

### 4.3 Integrity

For each fetched object:
- compute ValueID from bytes
- verify it matches the requested ValueID

This provides end-to-end integrity.

---

## 5) Bandwidth bounds (evaluation story)

Let:
- R = bytes of row/version metadata shipped (WAL records excluding inlined values)
- U = total bytes of unique value objects referenced by those records
- I = bytes of value objects already present on the receiver

Then total bytes transmitted with CAS-aware replication is approximately:

B_cas = R + (U - I) + overhead

Naive inlining replication would transmit:

B_naive = R + U + overhead

Thus savings is:

S = B_naive - B_cas = I

Interpretation:
- savings equals the bytes of referenced objects already present on the receiver.
- CAS-assisted replication is maximally beneficial when:
  - replicas share many objects due to deduplication,
  - delta chains share common bases,
  - shard rebalancing moves data that overlaps previously hosted data.

---

## 6) Shard move / rebalance acceleration

For Level-3 shard moves, the same mechanism applies:
- sender enumerates row versions for the shard
- sender sends referenced ValueIDs (or their Bloom summary)
- receiver requests only missing objects

Optimization:
- "object manifest" per shard: a compact set of ValueIDs referenced by live row versions
- manifests allow prefetch and accurate progress reporting

---

## 7) Metrics

Expose per link and per node:
- repl_ref_bytes_total (bytes of reference/WAL stream)
- repl_obj_bytes_total (bytes of value objects transferred)
- repl_obj_saved_bytes_total (estimated bytes saved by CAS; equals I estimate)
- repl_obj_hit_rate (fraction of referenced ValueIDs already present)
- repl_missing_batch_size_avg
- repl_apply_lag_lsn

These metrics make the feature publishable: they quantify bandwidth savings.

---

## 8) Backlog

- CR01: ValueID existence Bloom summaries
- CR02: object fetch protocol + batching
- CR03: replication metrics (saved bytes, hit rate)
- CR04: shard move uses object manifests + progress reporting
