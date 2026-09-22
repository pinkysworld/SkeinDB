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

### HTTP-over-TCP transfer validation

`eval/two_node_http_wire_validation.py` removes the remaining application-layer byte-accounting gap. It inserts a transparent raw TCP proxy into the same two-node `objects.pull -> objects.fetch` path and compares each CAS-overlap scenario to a zero-overlap destination using the same source object set.

The proxy counts exact TCP payload bytes in both directions. That includes HTTP framing, JSON-RPC envelopes, ValueID request bodies, both Base64 payload fields, and JSON metadata. It excludes lower-layer Ethernet/IP/TCP headers, packet-level retransmissions, and TLS.

| Scenario | Baseline HTTP-over-TCP bytes | CAS HTTP-over-TCP bytes | HTTP savings | ValueStore-object savings | HTTP / object expansion |
|---|---:|---:|---:|---:|---:|
| low redundancy / high churn | 143,359 | 107,381 | 25.1% | 25.1% | 2.54x |
| balanced | 76,214 | 30,487 | 60.0% | 60.0% | 2.55x |
| high redundancy / low churn | 16,887 | 2,611 | 84.5% | 86.7% | 2.95x at the tiny residual transfer |

Two findings matter:

1. **CAS savings survive protocol overhead.** At 25% and 60% overlap, measured HTTP savings differ from ValueStore-byte savings by only 0.002 percentage points.
2. **The pre-CR05 fetch encoding had measurable amplification.** The preserved baseline used about 2.54-2.55 HTTP-over-TCP bytes per ValueStore object byte because the response carried both `bytes_b64` and `entry_b64` plus metadata. CR05 introduces a backward-compatible `transfer_only` response used by `objects.pull`, leaving legacy direct fetches unchanged.

A second identical pull still opens zero proxy connections and transfers exactly zero HTTP bytes.

The checked-in evidence lives in `eval/reports/cas_http_wire_ci.json` and `eval/reports/cas_http_wire_ci.md`. Both files are regenerated and diff-gated in CI.

#### CR05 before/after

The original wire report is retained as `eval/reports/cas_http_wire_pre_cr05.{json,md}`. Re-running the identical harness after CR05 yields:

| Scenario | Pre-CR05 zero-overlap | Post-CR05 zero-overlap | Reduction | Post-CR05 wire/object |
|---|---:|---:|---:|---:|
| low redundancy / high churn | 143,359 B | 56,564 B | 60.5% | 1.00x |
| balanced | 76,214 B | 30,280 B | 60.3% | 1.01x |
| high redundancy / low churn | 16,887 B | 6,677 B | 60.5% | 1.01x |

The CAS savings signal itself is preserved: 25.1% and 60.0% overlap savings remain essentially identical to the ValueStore-byte savings. At the tiny four-object residual transfer, HTTP savings are 81.0% versus 86.7% object-byte savings because fixed request overhead becomes dominant.

The compatibility probe confirms that legacy fetch fields remain present by default, compact mode contains only `id + entry_b64`, and the canonical `entry_b64` payload is identical in both modes.

#### CR06 connection reuse

After CR05 removed redundant payload bytes, the next measurable overhead was connection churn. Before CR06, `fetch_remote_object_batch` constructed a fresh `reqwest::Client` for every batch, so the transparent proxy observed one TCP connection per batch.

CR06 moves client construction to the lifetime of one `objects.pull`. The byte counts remain unchanged, while the connection count becomes one for every non-empty pull:

| Scenario | Baseline batches / connections before | Baseline connections after | CAS batches / connections before | CAS connections after |
|---|---:|---:|---:|---:|
| low redundancy / high churn | 8 / 8 | **1** | 6 / 6 | **1** |
| balanced | 5 / 5 | **1** | 2 / 2 | **1** |
| high redundancy / low churn | 1 / 1 | **1** | 1 / 1 | **1** |

The pre-CR06 report is retained as `eval/reports/cas_http_wire_pre_cr06.{json,md}`. The current report includes connection counts and is CI-diffed. No latency or throughput improvement is claimed from the loopback test; the verified claim is deterministic connection reuse with unchanged transfer correctness and bytes.

#### CR07 batch-size sweep

With payload duplication and connection churn already removed, CR07 measures the remaining per-request framing cost by sweeping batch sizes 16, 32, 64, 128, and 256 on the same raw-TCP proxy path.

Across the low-redundancy/high-churn and balanced scenarios, combined zero-overlap plus CAS-overlap HTTP bytes are 150,108, 141,327, 136,927, 135,159, and 133,835 bytes respectively. Moving from 32 to the existing default 64 saves 3.11%. Moving from 64 to 128 saves only another 1.29%, while 64 to 256 saves another 2.26%.

The existing default of **64** is therefore retained. It captures most of the measurable per-request byte reduction while avoiding unnecessarily large single responses and coarse retry units. Explicit callers can still select other supported batch sizes.

The deterministic report is checked in as `eval/reports/cas_batch_sweep_ci.{json,md}` and regenerated in CI. The result is scoped to byte efficiency; it is not a latency or throughput benchmark.

#### CR08 wire composition

CR08 parses the captured HTTP stream itself rather than treating the proxy total as one opaque number. At the validated batch size 64, the low-redundancy/high-churn CAS transfer consists of 41,050 bytes: 22,534 bytes (54.9%) are the JSON-quoted `entry_b64` payloads, 6,688 bytes (16.3%) are request ValueIDs, 6,494 bytes (15.8%) are repeated response ValueIDs, and the remaining request/response HTTP + JSON-RPC structure accounts for 5,334 bytes. The balanced case has nearly identical percentages.

The measured Base64 text expansion is 1.349x over decoded transfer entries. HTTP headers are small relative to the remaining payload, so further header or connection tuning is not the main optimization opportunity.

This evidence motivates the next staged transport change: a backward-compatible binary replication response carrying canonical transfer-entry bytes directly. Because each canonical transfer entry already embeds its ValueID, such a response can remove both Base64 expansion and repeated response-ID JSON. Raw 16-byte request IDs are a separate later optimization.

The checked-in report is `eval/reports/wire_composition_ci.{json,md}`, and CI requires its categories to reproduce exactly.

#### CR09 binary response

CR09 replaces the Base64/JSON response selected by CR08 with a small binary envelope containing canonical transfer-entry bytes. The request remains JSON, and new destinations automatically fall back to the legacy compact JSON-RPC fetch when a source does not expose the binary endpoint.

At batch 32, zero-overlap transfers fall from 56,564 to 34,361 bytes (low redundancy), 30,280 to 18,426 bytes (balanced), and 6,677 to 4,059 bytes (high redundancy), roughly **39% below the pre-CR09 path**. CAS-overlap transfers fall from 42,370 to 25,739 bytes and 12,113 to 7,371 bytes in the two larger scenarios.

At batch 64, the remaining low-redundancy CAS stream is 24,806 bytes: 16,424 bytes (66.2%) are canonical binary transfer entries and 6,688 bytes (27.0%) are still hexadecimal request ValueIDs. The balanced case is 7,061 bytes with 65.8% transfer entries and 26.8% request IDs.

That moves the next optimization target from the response to the request: raw 16-byte ValueIDs can remove the remaining hexadecimal JSON expansion without changing the canonical object representation. The pre-CR09 and post-CR09 reports are retained separately for direct reproducibility.
