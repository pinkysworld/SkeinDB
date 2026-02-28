# SkeinDB True Status Matrix

Last updated: 2026-02-28

This matrix reconciles runtime reality with backlog checklists.

Interpretation:
- **Implemented**: shipped in runtime and exercised by tests.
- **Partial / Prototype**: usable surfaces exist, but hardening/completeness tasks remain open.
- **Planned**: backlog intent exists, but no meaningful runtime implementation yet.

## 1) Backlog checklist snapshot

- `docs/PROJECT_BACKLOG.md`: **34 done / 71 open** (105 total)
- `docs/RESEARCH_BACKLOG.md`: **0 done / 109 open**

Why `RESEARCH_BACKLOG` still shows 0 done: those checklists now represent
publication-grade hardening/evaluation tasks; prototype runtime coverage is tracked below.

## 2) Core roadmap (Phases 0-23)

| Phase | Current status | Notes / evidence |
|---|---|---|
| Phase 0 Repo setup | Implemented | T001/T002/T003 are complete in runtime and tests: VarU/CRC/value ID primitives, FileHeader read/write, and RecordFrame append/iterate (`crates/skeindb-core/src/lib.rs`, `crates/skeindb-core/tests/phase0_format.rs`). |
| Phase 1 Storage core | Partial | Prototype engine persists JSON state and includes adaptive ValueID-backed row ref encoding for table files (`format_version: 2`, emits `"$skein_ref"` only when byte-profitable); full MANIFEST/WAL/LSM pipeline remains open. |
| Phase 2 SQL + metadata | Partial | Catalog + minimal CREATE/INSERT/SELECT paths are implemented (`engine`, `sql.exec`), including virtual `information_schema.tables` and `information_schema.columns`. |
| Phase 3 MySQL protocol | Implemented (baseline) | Wire listener performs handshake + `mysql_native_password` auth exchange and supports `COM_QUERY` through the SQL-translation subset (`SELECT/SHOW/USE/CREATE DATABASE/CREATE TABLE/ALTER TABLE ... ADD COLUMN/DROP TABLE/INSERT/UPDATE/DELETE`, `INSERT IGNORE`, `REPLACE`, `INSERT ... ON DUPLICATE KEY UPDATE`, `DISTINCT`, simple `INNER JOIN`, and `SQL_CALC_FOUND_ROWS` + `FOUND_ROWS()`). The MySQL layer now also preserves MySQL-style column defaults for `CREATE TABLE` / `ALTER TABLE`, surfaces them through `SHOW FULL COLUMNS` / `SHOW CREATE TABLE`, and uses a corpus-oriented leading-column duplicate-key emulation for WordPress-like `wp_options` traffic. `tests/compat/corpus.sql` runs end-to-end over the MySQL listener in integration tests. |
| Phase 4 Web console | Partial | `/api/v1/sql/exec` HTTP endpoint is live and console scaffold + schema/sql workspace exist (`web/skeinadmin`). |
| Phase 5 SkeinQL API | Implemented (baseline) | Typed SkeinQL models + `/api/v1/rpc` + `schema.*` (list/create/describe/drop) + `query.select` + `tx.begin/tx.commit/tx.rollback` are implemented. |
| Phase 6 ETag cache coherence | Partial | `data.get` ETag / `If-Match` updates + dependency-aware query paths exist. |
| Phase 7 Delta values | Implemented (prototype) | DELTA kind, policy/metrics, and compaction behavior are implemented in ValueStore tests. |
| Phase 8 Wasm extensions | Partial | Wasm plan compile/run + merge wasm registry exist; full UDF surface is still open. |
| Phase 9 Audit WAL | Partial | Forensic/audit prototype exists; full WAL v2 chain + checkpoint anchors remain open. |
| Phase 10 Row/column snapshots | Partial | Snapshot build/read/incremental paths exist; optimizer coverage still evolving. |
| Phase 11 Compat telemetry + migration | Partial | Migration intent/rewrite surfaces exist (`migration.*`), full telemetry suite remains open. |
| Phase 12 SkeinAdmin | Partial | SkeinAdmin is embedded and functional; security/observability hardening remains open. |
| Phase 13 Observability | Partial | `stats.snapshot`, `stats.top_queries`, `stats.slow_queries`, and `/metrics` exist, including live dedup storage metrics (`dedup_ratio`, logical/unique/duplicate bytes); deeper latency histogram surfaces remain open. |
| Phase 14 Cluster scale-out | Partial (advanced) | Node identity, fanout replication, shard metadata, and cluster RPCs are implemented. |
| Phase 15 Perf improvements | Planned | Interned-column/late-mat/vectorized pipeline items remain backlog work. |
| Phase 16 Query coalescing | Partial | In-flight coalescing exists (not yet complete for all planned entry points). |
| Phase 17 CAS-aware replication | Planned | Object-level pull/bloom savings protocol is still backlog work. |
| Phase 18 Index advisor | Partial | `advisor.*` methods and prototype workflow exist; full lifecycle/reporting remains open. |
| Phase 19 Time travel + replay | Partial | Replay/time-travel prototype surfaces/docs exist; full SQL+UI coverage remains open. |
| Phase 20 Encryption | Planned | Dedup-preserving encryption backlog remains open. |
| Phase 21 Compaction scheduler | Partial | Policy scaffolding/docs exist; full constrained scheduler/evaluation remains open. |
| Phase 22 Autoparam + plan cache | Partial | SQL normalization/classification prototype exists (`ai.autoparam.*`), full session/plan-cache integration remains open. |
| Phase 23 CDC/changefeeds | Partial | `cdc.subscribe_table` + `cdc.poll` are implemented; query subscriptions/streaming/ack remain open. |

## 3) Research tracks (R01-R20)

| Track | Runtime status | Primary runtime surface |
|---|---|---|
| R01 Learned indexes | Prototype implemented | ValueStore learned index scaffolding + tests (`skeindb-core/valuestore`). |
| R02 Adaptive row/column | Prototype implemented | Snapshot surfaces and hybrid execution scaffolds. |
| R03 Delta topology | Prototype implemented | Delta-chain policy/skip/compaction paths. |
| R04 Differential privacy | Implemented (prototype) | `dp.*` endpoints + budget/audit behaviors. |
| R05 Oblivious execution | Prototype implemented | `oblivious.policy.*`, `oblivious.explain`. |
| R06 Forensic WAL queries | Prototype implemented | `forensic.query`, `forensic.verify`, `forensic.export`. |
| R07 Client-side merge funcs | Prototype implemented | `merge.*`, `merge.wasm.*`, conflict handling paths. |
| R08 Incremental views | Prototype implemented | `view.create/drop/refresh/status/explain_deps`. |
| R09 QUIC-native protocol | Implemented (prototype) | QUIC transport + integration tests (`tests/quic_rpc.rs`). |
| R10 Vector embeddings | Prototype implemented | `vector.insert/search/index.status`. |
| R11 LLM/autoparam | Prototype implemented | `ai.autoparam.classify/analyze`. |
| R12 NL -> SkeinQL | Prototype implemented | `ai.nl.translate/explain/execute`. |
| R13 Causal ETag consistency | Prototype implemented | ETag/min-causality controls in query paths. |
| R14 Replay bundles | Prototype implemented | Replay/time-travel docs + prototype surfaces. |
| R15 Schema evolution | Prototype implemented | `schema.propose_change/merge_status/apply_merge`. |
| R16 Auto index synthesis | Prototype implemented | `advisor.index_synthesize/apply_index/history`. |
| R17 Intent inference | Prototype implemented | `migration.intent_report/rewrite_preview`. |
| R18 Perf regression replay | Prototype implemented | Replay/perf scaffolds and harness direction. |
| R19 Wasm query operators | Prototype implemented | `wasm.plan.compile/run` + batch ABI scaffolds. |
| R20 Energy-aware compaction | Prototype implemented | Policy-level scheduler scaffolds and docs. |

## 4) Recommended “next truth-maintenance” rule

When a task is promoted from prototype to hardened behavior, update:
1. The corresponding checkbox in `docs/PROJECT_BACKLOG.md` or `docs/RESEARCH_BACKLOG.md`.
2. This matrix row (status + evidence pointer).
3. At least one test reference proving the claim.
