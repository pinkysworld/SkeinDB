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

A future live-engine extension should replace this analytical identity/replication layer with measurements captured directly from:

- `stats.snapshot.storage`;
- `cluster.replication_stats`;
- actual `query.select` and `query.patch` response bodies.

That live extension can reuse the same report schema while changing the evidence source from analytical to runtime.
