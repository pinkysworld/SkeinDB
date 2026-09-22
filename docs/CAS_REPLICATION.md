# CAS-aware Replication and Bandwidth Bounds

Status: Prototype
Last updated: 2026-04-20

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
- `objects.pull` (replica-side orchestration): batch locally-missing ValueIDs, call remote `objects.fetch`, verify each fetched object against its requested ValueID, and persist only validated entries

An alternative is replica-initiated pull:
- replica requests missing ValueIDs directly when apply fails due to missing objects

Batching is recommended to amortize overhead.

Current prototype coverage:

- `objects.fetch` now returns a lossless VE1 payload (`entry_b64`) in addition to the legacy raw-bytes view, so non-trivial `ValueStore` entries such as deltas can be reconstructed faithfully
- `objects.pull` batches remote fetches, skips locally present objects, and recursively pulls missing delta-base dependencies before import
- verified entries are imported via the same `ValueStore` decoding path used for `.vseg` replay
- shard transfer preflight now includes `cluster.shard.manifest`, which enumerates the ValueIDs referenced by the live rows of a shard scope (database or table)
- `cluster.shard.move` and `cluster.shard.rebalance` now use shard manifests plus `objects.need` / `objects.pull` to transfer only missing objects before placement changes, and return manifest/progress summaries with object counts, bytes, batches, and pull outcomes

### 4.3 Integrity

For each fetched object:
- decode the transferred VE1 entry
- materialize full bytes (following delta bases when necessary)
- compute ValueID from the materialized bytes
- verify it matches the requested ValueID before import

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

Current prototype coverage:

- shard manifests are computed from the source node's live rows, either locally or through the internal `cluster.shard.manifest` RPC when the source primary is remote
- the destination node answers `objects.need` against the manifest, so move planning can distinguish already-present objects from the transfer set
- non-dry-run moves call `objects.pull` on the destination and only update shard placement after the required objects were stored successfully
- `cluster.shard.move` and `cluster.shard.rebalance` return `manifest` and `progress` sections so callers can report total bytes, missing bytes, batches, fetched objects, stored objects, and verification failures

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

For a deterministic cross-layer workload that combines this documented CAS byte bound with storage deduplication and QueryPatch delivery, see [REDUNDANCY_PIPELINE_EVAL.md](REDUNDANCY_PIPELINE_EVAL.md). Its original CAS values are analytical object-byte savings.

A follow-up two-node runtime experiment now exercises the production `objects.pull -> objects.fetch` path over real HTTP between two independent SkeinDB processes. The destination is pre-seeded with a deterministic subset of the source ValueIDs and then requests the full object set. For 300-row low/balanced/high-redundancy scenarios, the measured ValueStore object-byte savings are **25.1%**, **60.0%**, and **86.7%**. The first pull fetched/stored 191, 54, and 4 missing objects respectively with zero invalid IDs, zero remote-missing IDs, and zero verification failures. A second identical pull performed zero remote fetches and transferred zero object bytes in all scenarios.

The checked-in evidence is `eval/reports/cas_two_node_ci.{json,md}`, produced by `eval/two_node_cas_validation.py` and snapshot-diffed in CI. These are production ValueStore entry-byte counters, not packet-capture bytes; HTTP/TCP framing, TLS, compression, latency, and throughput remain outside the claim.

A second experiment, `eval/two_node_http_wire_validation.py`, now measures the actual plain-HTTP transfer size seen on the TCP stream. A transparent proxy counts every TCP payload byte between destination and source, including HTTP request/status lines, headers, JSON-RPC envelopes, ValueID request data, Base64 payloads, and JSON metadata.

| Scenario | 0%-overlap HTTP bytes | CAS-overlap HTTP bytes | HTTP bytes saved | ValueStore bytes saved |
|---|---:|---:|---:|---:|
| low redundancy / high churn | 143,359 | 107,381 | 25.1% | 25.1% |
| balanced | 76,214 | 30,487 | 60.0% | 60.0% |
| high redundancy / low churn | 16,887 | 2,611 | 84.5% | 86.7% |

For the larger transfers, CAS object-byte savings survive almost exactly at the HTTP layer. At very high overlap the fixed request/header/RPC cost becomes visible: 86.7% object-byte savings become 84.5% HTTP-over-TCP savings.

The current `objects.fetch` representation expands ValueStore object bytes to about **2.54-2.55x** total HTTP-over-TCP payload for the larger baseline transfers (about 39% ValueStore-byte efficiency). The response carries both `bytes_b64` and `entry_b64` plus JSON/RPC metadata; `objects.pull` consumes `entry_b64` for transfer verification/import. This measured amplification is now an explicit optimization target rather than an inferred cost.

The checked-in evidence is `eval/reports/cas_http_wire_ci.{json,md}` and is byte-for-byte regenerated in CI. The measurement excludes Ethernet/IP/TCP packet headers, lower-layer retransmission accounting, and TLS because the CI experiment uses plain loopback HTTP.

### 7.2) CR05 compact transfer-only fetch

CR05 keeps the public legacy `objects.fetch` response compatible while adding an optional compact request flag:

```json
{
  "method": "objects.fetch",
  "params": {
    "ids": ["<value-id>"],
    "transfer_only": true
  }
}
```

Legacy mode still returns `id`, `bytes_b64`, `entry_b64`, `kind`, and `verified`. Compact mode returns only `id` and the canonical `entry_b64`. The live harness verifies that `entry_b64` is byte-for-byte identical between the two modes. `objects.pull` now requests compact mode automatically. Older source nodes remain compatible because the pre-CR05 parameter parser ignores unknown request fields and returns the legacy superset, which the pull decoder still accepts.

The preserved pre-CR05 snapshot is `eval/reports/cas_http_wire_pre_cr05.{json,md}`. With the same workload and batch size, CR05 changes the measured wire path as follows:

| Scenario | Pre-CR05 baseline HTTP bytes | CR05 baseline HTTP bytes | Transfer reduction | Pre-CR05 wire/object | CR05 wire/object |
|---|---:|---:|---:|---:|---:|
| low redundancy / high churn | 143,359 | 56,564 | 60.5% | 2.54x | 1.00x |
| balanced | 76,214 | 30,280 | 60.3% | 2.55x | 1.01x |
| high redundancy / low churn | 16,887 | 6,677 | 60.5% | 2.55x | 1.01x |

For the CAS-overlap transfers themselves, low and balanced scenarios fall from 107,381 -> 42,370 bytes and 30,487 -> 12,113 bytes respectively, also about 60% lower. The tiny four-object residual transfer falls from 2,611 -> 1,267 bytes; fixed request/header costs dominate there.

For the larger transfers, ValueStore-byte efficiency rises from roughly **39% pre-CR05 to 98.5-99.6% post-CR05**. The optimization changes representation only; object verification, recursive delta-base fetching, post-transfer completeness, and idempotent second pulls remain unchanged.

### 7.3) CR06 persistent pull client

CR06 reuses one `reqwest::Client` for the complete `objects.pull` operation instead of constructing a new client for every batch. Fetches remain sequential, so the same HTTP/1.1 keep-alive connection can carry all remote `objects.fetch` batches for one pull.

The preserved pre-CR06 snapshot is `eval/reports/cas_http_wire_pre_cr06.{json,md}`. With the same CR05 transfer format and 32-object batch size, CR06 leaves application bytes unchanged but collapses connection churn:

| Scenario | Pull side | Batches | Pre-CR06 TCP connections | CR06 TCP connections |
|---|---|---:|---:|---:|
| low redundancy / high churn | zero-overlap baseline | 8 | 8 | **1** |
| low redundancy / high churn | CAS overlap | 6 | 6 | **1** |
| balanced | zero-overlap baseline | 5 | 5 | **1** |
| balanced | CAS overlap | 2 | 2 | **1** |
| high redundancy / low churn | zero-overlap baseline | 1 | 1 | **1** |
| high redundancy / low churn | CAS overlap | 1 | 1 | **1** |

The raw HTTP-over-TCP byte counts remain 56,564 / 42,370 B, 30,280 / 12,113 B, and 6,677 / 1,267 B for baseline / overlap respectively. CR06 is therefore a connection-reuse optimization rather than a byte-compression claim.

The live wire harness now fails if any non-empty pull opens more than one source TCP connection. Fully synchronized second pulls still open zero remote connections and transfer zero bytes.

### 7.1) `cluster.replication_stats` RPC (T167)

The runtime counters behind these metrics are exposed via the
read-only RPC `cluster.replication_stats`, and embedded into
`stats.snapshot` under `cluster.replication_objects`. Shape:

```json
{
  "need_calls": 12, "need_ids_total": 480, "need_hits": 420, "need_misses": 60,
  "missing_calls": 3, "missing_ids_total": 120, "missing_hits": 100, "missing_misses": 20,
  "fetch_calls": 3, "fetch_ids_total": 60, "fetch_objects_served": 60,
  "ref_bytes": 4096000,
  "obj_bytes": 524288,
  "saved_bytes": 4096000,
  "hit_rate": 0.875,
  "saved_bytes_ratio": 0.887,
  "last_updated_ms": 1758700000000
}
```

- `ref_bytes` = total bytes of objects the local replica already had when
  asked about them via `objects.need` (= bytes avoided on the wire thanks
  to CAS dedup).
- `obj_bytes` = total bytes actually served via `objects.fetch`.
- `hit_rate` = `need_hits / (need_hits + need_misses)`.
- `saved_bytes_ratio` = `ref_bytes / (ref_bytes + obj_bytes)`.

Counters are updated inside the `objects.need` / `objects.missing` /
`objects.fetch` handlers regardless of caller role, so both primary and
replica sides see the local CAS cost model in real time.

---

## 8) Backlog

- CR01: ValueID existence Bloom summaries
- CR02: object fetch protocol + batching (implemented via `objects.fetch` + `objects.pull`)
- CR03: replication metrics (saved bytes, hit rate)
- CR04: shard move uses object manifests + progress reporting (implemented via `cluster.shard.manifest`, `cluster.shard.move`, and `cluster.shard.rebalance`)\n- CR05: compact replication fetch representation (implemented): `objects.pull` requests `transfer_only: true`; legacy direct `objects.fetch` remains unchanged by default; CI preserves pre-CR05 and post-CR05 HTTP-wire snapshots.\n- CR06: persistent pull HTTP client (implemented): one `reqwest::Client` per `objects.pull`, with CI requiring one source TCP connection for every non-empty multi-batch pull.
