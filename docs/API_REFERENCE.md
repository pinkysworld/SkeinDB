# SkeinDB API Reference

Last updated: 2026-05-27
Runtime baseline: v0.3.17 plus current main, SkeinQL 1.0, 145 advertised RPC methods

This page is the practical API map for clients that talk to SkeinDB directly. The normative language and data model live in `SKEINQL.md`; this reference summarizes endpoints, method families, stability, result formats, and client behavior that should stay consistent across HTTP, QUIC, and embedded admin calls.

## Transport endpoints

| Surface | Endpoint | Purpose | Notes |
| --- | --- | --- | --- |
| SkeinQL RPC | `POST /api/v1/rpc` | JSON-RPC-style request/response envelope | Always inspect the envelope `ok` flag; HTTP 200 may contain an RPC error. |
| Prepared query GET | `GET /api/v1/q/{query_id}` | Cacheable prepared-query execution | Supports `If-None-Match` and returns ETags. |
| CDC SSE | `GET /api/v1/cdc/sse/{sub_id}` | Server-Sent Events stream for table/query subscriptions | Supports `Last-Event-ID` and emits `resnapshot` when the retention horizon is missed. |
| CDC WebSocket | `GET /api/v1/cdc/ws/{sub_id}` | WebSocket stream for table/query subscriptions | Accepts `Last-Event-ID` or `from_offset` and sends JSON text-frame envelopes ending with `resnapshot` on horizon loss. |
| MySQL wire | TCP port 3306 by default | Compatibility adoption surface | MySQL behavior is corpus-backed, not claimed as 100% parity. |
| PostgreSQL wire | TCP port 5432 by default | Early PG v3 compatibility surface | Partial baseline: startup/auth, simple query, extended query, catalog probes, transactions/savepoints. |
| QUIC RPC | Configured QUIC listener | SkeinQL over QUIC | Mirrors core RPC behavior and zero-RTT write rejection tests. |

## Request and response contract

Every SkeinQL RPC request is an object:

```json
{
  "skeinql": "1.0",
  "id": "req-1",
  "method": "query.select",
  "params": {}
}
```

Successful response:

```json
{"id":"req-1","ok":true,"result":{}}
```

Error response:

```json
{"id":"req-1","ok":false,"error":{"code":"invalid_request","message":"Missing field: query","details":{"field":"query"}}}
```

Clients should treat `id` as opaque, send `skeinql:"1.0"`, and use `system.capabilities` before relying on experimental families.

## Result formats

| Format | Use it for | Shape |
| --- | --- | --- |
| `rows_json` | Compact row arrays and table-like UI | `columns[]` plus `rows[][]` typed literals. |
| `objects_json` | Application code and admin panels | Array/object rows keyed by column names. |
| `skeinpack_v1` | Traffic reduction with known ValueIDs | Dictionary-backed wire object returned under `wire`. |
| `wasm_batch_v1` | Wasm operator batches | `format:"skein.wasm.batch.v1"` plus column metadata and packed rows. |

CDC subscriptions have their own event payload formats. `objects_json` is the default and returns `pk`, `before`, and `after` values as typed SkeinQL `Lit` envelopes; `plain_json` returns those fields as ordinary JSON scalars/objects for `cdc.poll`, SSE, and WebSocket delivery.

Cache-aware query methods can return `etag`, `deps`, `causality`, and `not_modified`. Use `min_causality` with `vector_clock_v2` tokens when a client must observe at least the writes from an earlier read; the runtime still accepts legacy `etag_chain_v1` tokens on input for compatibility.

## Method families

| Family | Methods | Stability | Notes |
| --- | --- | --- | --- |
| `system.*` | `system.ping`, `system.version`, `system.shutdown`, `system.capabilities` | Core | Capability discovery is the canonical method list. |
| `transport.*` | `transport.capabilities` | Core | Reports HTTP/QUIC availability. |
| `settings.*` | `settings.get`, `settings.list`, `settings.set` | Core | Runtime configuration surface. |
| `tx.*` | `tx.begin`, `tx.commit`, `tx.rollback` | Core | Snapshot/read-only flags; failures map to stable RPC errors. |
| `schema.*` | database/table DDL plus `schema.propose_change`, `schema.merge_status`, `schema.apply_merge` | Core + experimental | Conflict-free schema evolution is experimental. |
| `data.*` | `data.get`, `data.insert`, `data.update`, `data.delete` | Core | Typed literal row APIs. |
| `query.*` | `query.prepare`, `query.execute_prepared`, `query.select`, `query.patch`, `query.subscribe` | Core + experimental | ETags, query patches, prepared GET, and CDC hooks. |
| `sql.*` | `sql.exec` | Compatibility | SQL facade used by MySQL/PG/admin workflows. |
| `cdc.*` | `cdc.subscribe_table`, `cdc.subscribe_query`, `cdc.poll`, `cdc.pause`, `cdc.resume`, `cdc.ack`, `cdc.close` | Core | Subscriptions can optionally filter by exact primary-key tuple (`pk`), inclusive single-column primary-key ranges (`pk_range`), source ops (`ops`), and changed columns (`columns`), and can choose `objects_json` or `plain_json` event payloads. Subscribe results expose `sse_url` and `ws_url`; poll, SSE, and WebSocket share the same retained-event semantics, acknowledged CDC cursors and operator pauses persist across restart until closed, included row images are projected to the selected columns when `columns` is active, query subscriptions expand view dependencies to the underlying base tables that emit retained CDC events, traverse set-operation branches (for example `UNION` / `UNION ALL`) when deriving dependency tables, walk CTE definitions for base-table dependencies while ignoring CTE names as physical tables, and `backpressure` reports `healthy`, `pressure`, `throttle`, `paused`, or `resnapshot_required`. `stats.snapshot.cdc` exposes runtime feed counts, lag, retained-window offsets, pause/pressure counters, thresholds, and dropped-event totals. |
| `stats.*` | `stats.snapshot`, `stats.top_queries`, `stats.query_fingerprint_latency`, `stats.slow_queries`, `stats.coalescing` | Core | Live operational telemetry, including storage, cache, CDC, query summaries, and per-fingerprint latency histograms. |
| `cluster.*` | status, node lifecycle, shard create/move/rebalance, replica promote, replication stats | Experimental | Single-binary cluster control plane with replication counters and causal watermarks. |
| `objects.*` | `objects.need`, `objects.missing`, `objects.fetch`, `objects.pull` | Experimental | ValueID object transfer for shard moves and replication. |
| `settings.encryption.*` | status, mode, key registration, active key, rotation | Experimental | Envelope/key-management controls. |
| `dp.*` | aggregate, evaluate, budget, audit log | Experimental | Differential privacy COUNT/SUM/AVG aggregates, privacy ETags, budget accounting, audit, and accuracy-vs-epsilon evaluation. |
| `oblivious.*` | policy get/set, explain, evaluate | Experimental | Padding/shuffle access-pattern controls plus trace-based leakage and overhead reports. |
| `forensic.*` | query, verify, export | Experimental | Hash-chain audit, filtered forensic query, inclusion/boundary proof, and export bundle surfaces. |
| `merge.*` | register, apply, simulate, evaluate, Wasm registry | Experimental | Optimistic conflict resolution, values-only Wasm merge policies, and conflict workload evaluation. |
| `view.*` | create, drop, refresh, evaluate, status, explain deps | Experimental | Incremental/full/auto materialized views plus an incremental-vs-full oracle/benchmark report. |
| `vector.*` | insert, search, benchmark, index status | Experimental | Embedding storage, ANN search, and exact-vs-indexed recall/latency measurement. |
| `ai.*` | autoparam classification/analysis, NL translate/explain/execute | Experimental | Verification-gated natural-language workflows. |
| `advisor.*` | synthesize, apply, retire_unused, evaluate, dismiss, history | Experimental | Dependency-driven index suggestions, workload-shift convergence evaluation, equality/single-range/order/group latency benchmarks, and lifecycle controls. |
| `migration.*` | intent report, rewrite preview, report export | Experimental | Migration intent detection and reporting. |
| `maintenance.*` | audit, compaction, history GC, replay export/import/run | Core + experimental | Operational maintenance and replay bundles; replay export supports optional primary-key redaction. |
| `edge.*` | bundle request/apply/status | Experimental | Bounded replay bundles for edge replication. |
| `wasm.plan.*` | compile, inspect, edge_package, run | Prototype | Host-interpreted Wasm query-operator artifact ABI plus edge packaging helper. |
| `security.token.*` | create, list, revoke | Core | Bearer-token management. |
| `admin.user.*` | create, list, drop, grant, revoke | Core | Embedded admin user/role helpers. |

## `stats.snapshot` highlights

Important top-level sections in the current runtime surface:

- `query`: tracked call counts, slow-query count, average latency, `latency_ms.p50` / `latency_ms.p95` / `latency_ms.p99`, ETag hits, and coalescing totals.
- `stats.query_fingerprint_latency`: recent per-fingerprint drill-down with `p50_ms` / `p95_ms` / `p99_ms` plus bounded millisecond buckets from the same sample window.
- `cache`: plan-cache hit percentage plus hit and miss counters.
- `storage`: WAL-equivalent bytes, dedup metrics, total rows and tables, disk bytes, value-lookup histograms, and learned-index reports.
- `cdc`: `active_subscriptions`, `table_subscriptions`, `query_subscriptions`, `total_lag`, `max_lag`, `paused_subscriptions`, `pressured_subscriptions`, `throttle_recommended_subscriptions`, `warn_lag`, `throttle_lag`, `min_remaining_until_resnapshot`, `resnapshot_subscriptions`, `earliest_offset`, `latest_offset`, `retained_events`, `retention_limit`, and `dropped_events_total`.
- `compaction`: scheduler state, pressure, read/write latency percentiles, and workload-derived pacing decisions.
- `cluster`: cluster topology, replication state, and CAS object-transfer counters.

## Current advertised method set

The runtime advertises this set through `system.capabilities.methods` on current main:

```text
system.ping, system.version, system.shutdown, system.capabilities, transport.capabilities,
tx.begin, tx.commit, tx.rollback,
stats.snapshot, stats.top_queries, stats.query_fingerprint_latency, stats.slow_queries, stats.coalescing,
settings.get, settings.list, settings.set,
cluster.status, cluster.nodes, cluster.join_token.create, cluster.node.join, cluster.node.remove,
cluster.node.leave, cluster.replica.promote, cluster.shard.create, cluster.shard.move,
cluster.shard.rebalance, cluster.replication_stats,
objects.need, objects.missing, objects.fetch, objects.pull,
schema.list_databases, schema.create_database, schema.drop_database, schema.list_tables,
schema.create_table, schema.drop_table, schema.describe_table, schema.propose_change,
schema.merge_status, schema.apply_merge,
sql.exec, data.get, data.insert, data.update, data.delete,
query.prepare, query.execute_prepared, query.select, query.patch, query.subscribe,
cdc.subscribe_table, cdc.subscribe_query, cdc.poll, cdc.pause, cdc.resume, cdc.ack, cdc.close,
vector.insert, vector.search, vector.benchmark, vector.index.status,
ai.autoparam.classify, ai.autoparam.analyze, ai.nl.translate, ai.nl.explain, ai.nl.execute,
dp.aggregate, dp.evaluate, dp.budget.set, dp.budget.get, dp.audit.log,
oblivious.policy.set, oblivious.policy.get, oblivious.explain, oblivious.evaluate,
forensic.query, forensic.verify, forensic.export,
maintenance.audit_status, maintenance.audit_verify, maintenance.compaction.status,
maintenance.compaction.set_policy, maintenance.compaction.pause, maintenance.compaction.resume,
maintenance.history.status, maintenance.history.set_policy, maintenance.history.gc,
maintenance.replay.export, maintenance.replay.import, maintenance.replay.run,
settings.encryption.status, settings.encryption.set_mode, settings.encryption.register_key,
settings.encryption.set_active_key, settings.encryption.rotate_key,
edge.bundle.request, edge.bundle.apply, edge.bundle.status,
merge.register, merge.apply, merge.simulate, merge.evaluate, merge.wasm.register, merge.wasm.list, merge.wasm.drop,
wasm.plan.compile, wasm.plan.inspect, wasm.plan.edge_package, wasm.plan.run,
view.create, view.drop, view.refresh, view.evaluate, view.status, view.explain_deps,
advisor.index_synthesize, advisor.apply_index, advisor.retire_unused, advisor.evaluate, advisor.dismiss, advisor.history,
migration.intent_report, migration.rewrite_preview, migration.report_export,
telemetry.feature_flags, telemetry.compat_summary, telemetry.migration_hints, telemetry.workload_features,
plan_cache.status, plan_cache.clear,
security.token.create, security.token.list, security.token.revoke,
admin.user.create, admin.user.list, admin.user.drop, admin.user.grant, admin.user.revoke
```

## Client checklist

- Call `system.version` and `system.capabilities` at startup.
- Prefer typed literals over untyped JSON values.
- Use `query.prepare` plus `GET /api/v1/q/{query_id}` for cacheable read paths.
- Use `want_etag`, `if_none_match`, and `min_causality` for web/mobile clients that need coherent reads.
- Treat every experimental family as capability-gated.
- Surface `error.code` to retry logic; do not parse `message` for behavior.
- For compatibility migrations, collect samples through SQL/MySQL/PG paths, translate to SkeinQL ASTs, then run `migration.intent_report` and `migration.report_export`.