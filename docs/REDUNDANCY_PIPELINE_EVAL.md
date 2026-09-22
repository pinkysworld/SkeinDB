# Redundancy Pipeline Evaluation

Status: deterministic evaluation baseline  
Last updated: 2026-09-22

## Goal

SkeinDB has several mechanisms that reduce repeated bytes at different points in a database lifecycle. Evaluating each mechanism in isolation can make the project look like a feature collection, so this harness uses one workload to measure a single architectural theme:

**content identity is reused to avoid redundant storage, redundant object transfer, and redundant query-result transfer.**

The reproducible harness is `eval/redundancy_pipeline.py`.

## Layers

### 1. Storage

The storage layer models SkeinDB's adaptive cell references:

- repeated cell values share content identity;
- a first profitable occurrence carries the literal plus a fixed-width identity;
- later occurrences carry the identity only;
- values stay inline when the reference representation would be larger.

The reported scope is serialized **cell-value payload bytes**. It intentionally excludes row keys, file framing, indexes, WAL, and filesystem allocation, so this number must not be described as whole-database compression.

### 2. CAS-aware replication

The replication layer evaluates the bound documented in `docs/CAS_REPLICATION.md`.

Let:

- `U` be bytes in unique value objects referenced by the workload;
- `I` be bytes in those objects already present at the receiver.

The naive object-transfer baseline is `U`; CAS transfer is `U - I`; saved object bytes are therefore `I`.

The model excludes row/version metadata and protocol framing. It measures the object-transfer term only.

### 3. QueryPatch delivery

The delivery layer mutates the same base relation using deterministic add/update/remove operations and compares:

- a compact JSON full-result response;
- a compact JSON query patch carrying only changed rows/keys.

HTTP headers, TLS framing, gzip/brotli, and client parsing cost are excluded. If a patch would be larger than a full response, the lifecycle accounting selects the full response, matching the practical fallback strategy.

## Lifecycle byte-work

For one scenario the harness also reports:

```text
baseline byte-work =
    inline storage cell bytes
  + naive unique-object replication bytes
  + full query-response bytes

redundancy-aware byte-work =
    adaptive-ref storage cell bytes
  + CAS missing-object bytes
  + min(QueryPatch bytes, full response bytes)
```

This is a **cross-layer accounting metric**, not bytes on one physical network path. Its purpose is to make the architectural relationship measurable without conflating storage, replication, and client-delivery channels.

## Default deterministic scenarios

The checked-in default report uses 600 rows and seed `20260922`.

| Scenario | Payload repetition | Receiver object overlap | Change rate |
|---|---:|---:|---:|
| low_redundancy_high_churn | 15% | 25% | 20% |
| balanced | 55% | 60% | 5% |
| high_redundancy_low_churn | 90% | 85% | 1% |

Current checked-in model results:

| Scenario | Storage saved | CAS object bytes saved | QueryPatch saved | Lifecycle byte-work saved |
|---|---:|---:|---:|---:|
| low_redundancy_high_churn | 4.2% | 23.5% | 82.5% | 39.5% |
| balanced | 19.0% | 60.6% | 95.5% | 60.1% |
| high_redundancy_low_churn | 54.5% | 89.5% | 99.0% | 79.0% |

These are deterministic **model outputs**, not live latency or production-throughput measurements.

## Reproduce

```bash
python eval/redundancy_pipeline.py \
  --rows 600 \
  --seed 20260922 \
  --json-out eval/reports/redundancy_pipeline_default.json \
  --markdown-out eval/reports/redundancy_pipeline_default.md
```

Unit coverage lives in `eval/test_redundancy_pipeline.py`. CI regenerates the default JSON and Markdown outputs and diffs them against the checked-in reports, so changes to the byte model or workload are explicit reviewable changes.

## Identity implementation note

The runtime ValueID is BLAKE3-128. The Python harness uses a 128-bit stdlib digest surrogate because the evaluated properties here depend only on deterministic content equality and the fixed 32-hex-character identity width. It does **not** claim cryptographic equivalence to BLAKE3.

## Live-engine validation

The follow-up harness `eval/runtime_redundancy_validation.py` now runs the same three scenario families against a real SkeinDB process.

Evidence sources:

- storage: `stats.snapshot.storage.logical_bytes`, `unique_bytes`, and `duplicate_bytes`;
- CAS: real ValueIDs emitted by `skeinpack_v1`, real `objects.need` hit bytes, real `objects.fetch` object bytes, and `cluster.replication_stats`;
- query delivery: raw HTTP response-body bytes from `query.patch` versus a fresh full `query.select`.

The CAS experiment intentionally simulates receiver overlap on a single node by partitioning real ValueIDs into "already present" and "must fetch" sets. The byte accounting therefore exercises the real ValueStore RPC counters without claiming measured multi-node network throughput.

### Runtime results

The first CI-backed runtime snapshot uses 300 rows per scenario and seed `20260922`.

| Scenario | Runtime storage saved | Runtime CAS saved | Runtime QueryPatch saved | Model storage | Model CAS | Model QueryPatch |
|---|---:|---:|---:|---:|---:|---:|
| low redundancy / high churn | 15.0% | 25.1% | 64.6% | 4.2% | 23.3% | 82.5% |
| balanced | 55.0% | 60.0% | 84.1% | 19.0% | 57.4% | 95.2% |
| high redundancy / low churn | 90.0% | 86.7% | 98.2% | 54.5% | 88.3% | 98.7% |

The checked-in evidence is available in:

- `eval/reports/redundancy_runtime_ci.json`
- `eval/reports/redundancy_runtime_ci.md`

### What the model gets right and wrong

The CAS estimate is well calibrated in this workload: runtime savings differ from the analytical estimate by about +1.8, +2.6, and -1.6 percentage points across the three scenarios.

The QueryPatch model is optimistic when churn is high. The analytical patch model counts the core delta representation, while the live RPC measurement also carries columns, ETags, dependency and causality metadata, typed literals, and the response envelope. The gap narrows from about 17.9 percentage points at high churn to about 0.5 points at low churn.

The storage figures intentionally use different denominators. The analytical model estimates adaptive cell-reference encoding overhead, while the runtime counter reports logical ValueStore bytes versus unique ValueStore bytes. Runtime storage savings therefore validate the broader "repeated values collapse to unique content" behavior, but are not a direct validation of the on-disk reference-encoding model.

CI regenerates the live Markdown report and diffs it against the checked-in snapshot. A change to these runtime byte-saving results must therefore be reviewed explicitly.

### Two-node CAS transfer validation

The single-node CAS calibration is now complemented by `eval/two_node_cas_validation.py`, which starts two independent SkeinDB processes and drives the real remote `objects.pull -> objects.fetch` path.

The destination is populated with exact typed literals corresponding to a deterministic fraction of the source's ValueIDs. It then requests the complete source object set. The experiment uses production `cluster.replication_stats.ref_bytes` on the destination for bytes already present and the source's `obj_bytes` delta for bytes actually served during the remote pull.

| Scenario | Source ValueIDs | Pre-seeded | Remote fetched/stored | Saved object bytes | Remote fetches on identical second pull |
|---|---:|---:|---:|---:|---:|
| low redundancy / high churn | 255 | 64 | 191 / 191 | 25.1% | 0 |
| balanced | 135 | 81 | 54 / 54 | 60.0% | 0 |
| high redundancy / low churn | 30 | 26 | 4 / 4 | 86.7% | 0 |

Across all three scenarios:

- `objects.pull` reports zero invalid IDs, zero remote-missing IDs, and zero verification failures;
- the destination reports zero missing source ValueIDs after the transfer;
- the number of source `objects.fetch` calls matches the configured 32-object pull batches (6, 2, and 1);
- a second identical pull sees every object locally, executes zero transfer batches, makes zero source fetch calls, and transfers zero object bytes.

The reproducible evidence lives in `eval/reports/cas_two_node_ci.json` and `eval/reports/cas_two_node_ci.md`. CI regenerates the Markdown report and diffs it against the snapshot.

This closes the principal scope limitation of the earlier CAS calibration: the transfer path is now genuinely cross-process and remote. The byte denominator is still ValueStore entry bytes rather than a packet capture, so transport framing, TLS, compression, latency, and throughput remain deliberately unclaimed.
