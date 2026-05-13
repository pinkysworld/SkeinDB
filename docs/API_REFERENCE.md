# SkeinDB API Reference

Last updated: 2026-05-11
Runtime baseline: v0.3.17, SkeinQL 1.0, 133 advertised RPC methods

This page is the practical API map for clients that talk to SkeinDB directly. The normative language and data model live in `SKEINQL.md`; this reference summarizes endpoints, method families, stability, result formats, and client behavior that should stay consistent across HTTP, QUIC, and embedded admin calls.

## Transport endpoints

| Surface | Endpoint | Purpose | Notes |
| --- | --- | --- | --- |
| SkeinQL RPC | `POST /api/v1/rpc` | JSON-RPC-style request/response envelope | Always inspect the envelope `ok` flag; HTTP 200 may contain an RPC error. |
| Prepared query GET | `GET /api/v1/q/{query_id}` | Cacheable prepared-query execution | Supports `If-None-Match` and returns ETags. |
| CDC SSE | `GET /api/v1/cdc/sse/{sub_id}` | Server-Sent Events stream for table/query subscriptions | Supports `Last-Event-ID` and emits `resnapshot` when the retention horizon is missed. |
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

Cache-aware query methods can return `etag`, `deps`, `causality`, and `not_modified`. Use `min_causality` with `etag_chain_v1` tokens when a client must observe at least the writes from an earlier read.

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
| `cdc.*` | `cdc.subscribe_table`, `cdc.subscribe_query`, `cdc.poll`, `cdc.ack`, `cdc.close` | Core | Poll and SSE share event payloads. |
| `stats.*` | `stats.snapshot`, `stats.top_queries`, `stats.slow_queries`, `stats.coalescing` | Core | Live operational telemetry. |
| `cluster.*` | status, node lifecycle, shard create/move/rebalance, replica promote, replication stats | Experimental | Single-binary cluster control plane. |
| `objects.*` | `objects.need`, `objects.missing`, `objects.fetch`, `objects.pull` | Experimental | ValueID object transfer for shard moves and replication. |
| `settings.encryption.*` | status, mode, key registration, active key, rotation | Experimental | Envelope/key-management controls. |
| `dp.*` | aggregate, evaluate, budget, audit log | Experimental | Differential privacy COUNT/SUM/AVG aggregates, privacy ETags, budget accounting, audit, and accuracy-vs-epsilon evaluation. |
| `oblivious.*` | policy get/set, explain, evaluate | Experimental | Padding/shuffle access-pattern controls plus trace-based leakage and overhead reports. |
| `forensic.*` | query, verify, export | Experimental | Hash-chain audit, filtered forensic query, inclusion/boundary proof, and export bundle surfaces. |
| `merge.*` | register, apply, simulate, evaluate, Wasm registry | Experimental | Optimistic conflict resolution, values-only Wasm merge policies, and conflict workload evaluation. |
| `view.*` | create, drop, refresh, evaluate, status, explain deps | Experimental | Incremental/full/auto materialized views plus an incremental-vs-full oracle/benchmark report. |
| `vector.*` | insert, search, index status | Experimental | Embedding storage and ANN search. |
| `ai.*` | autoparam classification/analysis, NL translate/explain/execute | Experimental | Verification-gated natural-language workflows. |
| `advisor.*` | synthesize, apply, dismiss, history | Experimental | Dependency-driven index suggestions and lifecycle. |
| `migration.*` | intent report, rewrite preview, report export | Experimental | Migration intent detection and reporting. |
| `maintenance.*` | audit, compaction, history GC, replay export/import/run | Core + experimental | Operational maintenance and replay bundles; replay export supports optional primary-key redaction. |
| `edge.*` | bundle request/apply/status | Experimental | Bounded replay bundles for edge replication. |
| `wasm.plan.*` | compile, inspect, edge_package, run | Prototype | Host-interpreted Wasm query-operator artifact ABI plus edge packaging helper. |
| `security.token.*` | create, list, revoke | Core | Bearer-token management. |
| `admin.user.*` | create, list, drop, grant, revoke | Core | Embedded admin user/role helpers. |

## Current advertised method set

The runtime advertises this set through `system.capabilities.methods` in v0.3.17:

```text
system.ping, system.version, system.shutdown, system.capabilities, transport.capabilities,
tx.begin, tx.commit, tx.rollback,
stats.snapshot, stats.top_queries, stats.slow_queries, stats.coalescing,
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
cdc.subscribe_table, cdc.subscribe_query, cdc.poll, cdc.ack, cdc.close,
vector.insert, vector.search, vector.index.status,
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
advisor.index_synthesize, advisor.apply_index, advisor.dismiss, advisor.history,
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