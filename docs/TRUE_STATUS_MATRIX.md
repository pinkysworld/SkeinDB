# SkeinDB True Status Matrix

Last updated: 2026-03-27
Latest changes: prepared-query `GET /api/v1/q/{query_id}` is now covered as a real ETag / `304` HTTP surface with GET coalescing tests; CDC table subscriptions now support `cdc.ack` cursors and `cdc.close`; PostgreSQL compat phase planned.

This matrix reconciles runtime reality with backlog checklists.

Interpretation:
- **Implemented**: shipped in runtime and exercised by tests.
- **Partial / Prototype**: usable surfaces exist, but hardening/completeness tasks remain open.
- **Planned**: backlog intent exists, but no meaningful runtime implementation yet.

## 1) Backlog checklist snapshot

- `docs/PROJECT_BACKLOG.md`: **51 done / 86 open** (137 total; T037/T038 remain open but have significant progress notes; T400–T418 are new PostgreSQL compat phase)
- `docs/RESEARCH_BACKLOG.md`: **0 done / 109 open** (109 total)

Why `RESEARCH_BACKLOG` still shows 0 done: those checklists now represent
publication-grade hardening/evaluation tasks; prototype runtime coverage is tracked below.

## 2) Core roadmap (Phases 0-23)

| Phase | Current status | Notes / evidence |
|---|---|---|
| Phase 0 Repo setup | Implemented | T001/T002/T003 are complete in runtime and tests: VarU/CRC/value ID primitives, FileHeader read/write, and RecordFrame append/iterate (`crates/skeindb-core/src/lib.rs`, `crates/skeindb-core/tests/phase0_format.rs`). |
| Phase 1 Storage core | Partial | Prototype engine persists JSON state and includes adaptive ValueID-backed row ref encoding for table files (`format_version: 2`, emits `"$skein_ref"` only when byte-profitable); full MANIFEST/WAL/LSM pipeline remains open. |
| Phase 2 SQL + metadata | Partial | Catalog + minimal CREATE/INSERT/SELECT paths are implemented (`engine`, `sql.exec`), including virtual `information_schema.tables` and `information_schema.columns`. |
| Phase 3 MySQL protocol | Implemented (baseline) | Wire listener performs handshake + `mysql_native_password` auth exchange and supports `COM_QUERY` through the SQL-translation subset (`SELECT/SHOW/USE/CREATE DATABASE/DROP DATABASE/CREATE TABLE/CREATE [UNIQUE] INDEX/ALTER TABLE .../DROP INDEX/DROP TABLE/TRUNCATE TABLE/RENAME TABLE/INSERT/INSERT ... SELECT/UPDATE/DELETE`, `INSERT IGNORE`, `REPLACE`, `INSERT ... ON DUPLICATE KEY UPDATE`, multi-table `DELETE`, multi-table `UPDATE` (stub), `UNION`/`UNION ALL`, `DISTINCT`, `BETWEEN`/`NOT BETWEEN`, `REGEXP`/`RLIKE`/`NOT REGEXP`, `<=>` (NULL-safe equality), simple joins including `INNER`/`LEFT`/`RIGHT`/`CROSS JOIN`/`NATURAL JOIN`/`FULL OUTER JOIN` with multi-join chains, comma-separated `FROM` lists, `JOIN ... USING (...)`, derived tables (FROM subqueries), CTEs (`WITH...AS`), projection aliases, wildcard projections, aggregate shims for `COUNT(*)`/`COUNT(col)`/`COUNT(DISTINCT col)`/`SUM`/`MIN`/`MAX`/`AVG`/`GROUP_CONCAT()` with `HAVING`/`ORDER BY`/`LIMIT`/`OFFSET`, non-aggregate `GROUP BY` de-dup, broad scalar-function coverage including `CONCAT_WS`/`REPEAT`/`REVERSE`/`LPAD`/`RPAD`/`SPACE`/`HEX`/`UNHEX`/`FORMAT`/`SIGN`/`SQRT`/`POW`/`TRUNCATE`/`LOG`/`LN`/`LOG2`/`LOG10`/`EXP`/`PI`/`RAND`/`UUID`/`SLEEP`/`BENCHMARK`/`FIELD`/`ELT`/`INET_ATON`/`INET_NTOA`/`BIN`/`OCT`/`CONV`/`CRC32`/`MD5`/`SHA1`/`SHA`/`SHA2` plus the prior scalar baseline, JSON functions (`JSON_EXTRACT`/`JSON_UNQUOTE`/`JSON_OBJECT`/`JSON_ARRAY`/`JSON_CONTAINS`/`JSON_LENGTH`/`JSON_TYPE`/`JSON_VALID`/`JSON_SET`/`JSON_KEYS`/`JSON_MERGE_PRESERVE`), date/time functions including `STR_TO_DATE`/`WEEK`/`YEARWEEK`/`CONVERT_TZ`/`UTC_TIMESTAMP`/`UTC_DATE`/`UTC_TIME`/`SYSDATE`/`ADDTIME`/`SUBTIME`/`TIME_TO_SEC`/`SEC_TO_TIME` plus the prior date/time baseline, session functions `USER()`/`CURRENT_USER()`/`SESSION_USER()`/`SYSTEM_USER()`/`LAST_INSERT_ID()`/`CONNECTION_ID()`, `CASE`/`CAST`, arithmetic expressions, subquery rewrites, `SQL_CALC_FOUND_ROWS`/`FOUND_ROWS()`, `SELECT ... FOR UPDATE`/`FOR SHARE`/`LOCK IN SHARE MODE` locking hint stripping, `EXPLAIN` stub, `DO` statement, `CREATE VIEW`/`DROP VIEW` stubs, `SAVEPOINT`/`RELEASE SAVEPOINT`/`ROLLBACK TO SAVEPOINT` stubs, `SET GLOBAL` no-ops, `FLUSH`/`ANALYZE`/`OPTIMIZE`/`CHECK`/`REPAIR TABLE` no-ops, `KILL` no-op). Wire protocol includes `COM_INIT_DB` (0x02) and `COM_STATISTICS` (0x09). `SHOW` coverage expanded with `SHOW CREATE DATABASE`, `SHOW WARNINGS`/`ERRORS`, `SHOW PROCESSLIST`/`FULL PROCESSLIST`, `SHOW TRIGGERS`/`EVENTS`/`PROCEDURE STATUS`/`FUNCTION STATUS`, `SHOW PLUGINS`/`PROFILES`. `INFORMATION_SCHEMA` now includes `schemata` and `statistics` alongside `tables`/`columns`, with `information_schema.tables` carrying compatibility `TABLE_ROWS`/`DATA_LENGTH`/`INDEX_LENGTH` fields plus grouped storage-summary queries used by WordPress Site Health. The rest of the prior baseline (column defaults, `ALTER TABLE` variants, index enforcement, prepared-statement support, compatibility `SET`/`SHOW` forms, `LOCK TABLES`/`UNLOCK TABLES` no-ops, `NULL`-as-unknown semantics, secondary-index enforcement with `1062`/`23000` wire errors) is unchanged. `tests/compat/corpus.sql` runs end-to-end over the MySQL listener in integration tests. Corpus expanded from 772→1131 lines with over 375 SQL statements. Follow-on backlog items `T037`-`T038` track the remaining parity gaps. |
| Phase 4 Web console | Partial | `/api/v1/sql/exec` HTTP endpoint is live and console scaffold + schema/sql workspace exist (`web/skeinadmin`). |
| Phase 5 SkeinQL API | Implemented (baseline) | Typed SkeinQL models + `/api/v1/rpc` + `schema.*` (list/create/describe/drop) + `query.select` + `tx.begin/tx.commit/tx.rollback` are implemented. |
| Phase 6 ETag cache coherence | Partial | `data.get` ETag / `If-Match` updates, dependency-aware query paths, and prepared-query HTTP GET execution at `/api/v1/q/{query_id}` with ETag / `If-None-Match` (`304 Not Modified`) are implemented; live change subscriptions remain open. |
| Phase 7 Delta values | Implemented (prototype) | DELTA kind, policy/metrics, and compaction behavior are implemented in ValueStore tests. |
| Phase 8 Wasm extensions | Partial | Wasm plan compile/run + merge wasm registry exist; full UDF surface is still open. |
| Phase 9 Audit WAL | Partial | Forensic/audit prototype exists; full WAL v2 chain + checkpoint anchors remain open. |
| Phase 10 Row/column snapshots | Partial | Snapshot build/read/incremental paths exist; optimizer coverage still evolving. |
| Phase 11 Compat telemetry + migration | Partial | Migration intent/rewrite surfaces exist (`migration.*`), full telemetry suite remains open. |
| Phase 12 SkeinAdmin | Partial | SkeinAdmin is embedded and functional; security/observability hardening remains open. |
| Phase 13 Observability | Partial | `stats.snapshot`, `stats.top_queries`, `stats.slow_queries`, and `/metrics` exist, including live dedup storage metrics (`dedup_ratio`, logical/unique/duplicate bytes); deeper latency histogram surfaces remain open. |
| Phase 14 Cluster scale-out | Partial (advanced) | Node identity, fanout replication, shard metadata, and cluster RPCs are implemented. |
| Phase 15 Perf improvements | Planned | Interned-column/late-mat/vectorized pipeline items remain backlog work. |
| Phase 16 Query coalescing | Partial | In-flight coalescing exists for prepared-query HTTP GETs and `query.patch`; broader fingerprint/auth scoping, metrics/limits, and wider entry-point coverage remain open. |
| Phase 17 CAS-aware replication | Planned | Object-level pull/bloom savings protocol is still backlog work. |
| Phase 18 Index advisor | Partial | `advisor.*` methods and prototype workflow exist; full lifecycle/reporting remains open. |
| Phase 19 Time travel + replay | Partial | Replay/time-travel prototype surfaces/docs exist; full SQL+UI coverage remains open. |
| Phase 20 Encryption | Planned | Dedup-preserving encryption backlog remains open. |
| Phase 21 Compaction scheduler | Partial | Policy scaffolding/docs exist; full constrained scheduler/evaluation remains open. |
| Phase 22 Autoparam + plan cache | Partial | SQL normalization/classification prototype exists (`ai.autoparam.*`), full session/plan-cache integration remains open. |
| Phase 23 CDC/changefeeds | Partial | `cdc.subscribe_table`, `cdc.poll`, `cdc.ack`, and `cdc.close` are implemented for table subscriptions with in-memory ack cursors; query subscriptions, streaming transports, and retention/resnapshot remain open. |
| Phase 25 PostgreSQL compat | Planned | PG v3 wire protocol on port 5432, SCRAM-SHA-256 auth, PG SQL dialect translation, pg_catalog system tables, extended query protocol (Parse/Bind/Execute), PG type OIDs, RETURNING, dollar-quoting, deep dialect parity for psql/pgAdmin/Django/Rails/SQLAlchemy. Tasks T400–T418 in PROJECT_BACKLOG. |

## 3) Research tracks (R01-R20)

| Track | Runtime status | Primary runtime surface |
|---|---|---|
| R01 Learned indexes | Prototype implemented | ValueStore learned index scaffolding + tests (`skeindb-core/valuestore`). |
| R02 Adaptive row/column | Prototype implemented | Snapshot surfaces and hybrid execution scaffolds. |
| R03 Delta topology | Hardened | Delta-chain policy/skip/compaction paths + `topology_analysis()` with depth stats (avg/max/p50/p99), fanout, hot-chain detection, savings ratio. |
| R04 Differential privacy | Hardened | `dp.*` endpoints + budget/audit; crypto-quality hash-based PRNG (`DpRng`); Rényi DP composition tracking (`rdp_alphas`, `query_count`); `rdp_gaussian_cost()`, `rdp_laplace_cost()`, `rdp_to_eps_delta()`. |
| R05 Oblivious execution | Prototype implemented | `oblivious.policy.*`, `oblivious.explain`. |
| R06 Forensic WAL queries | Hardened | `forensic.query`, `forensic.verify`, `forensic.export`; Merkle tree root (`forensic_merkle_root()`), inclusion proofs (`forensic_merkle_proof()`), tamper detection. |
| R07 Client-side merge funcs | Prototype implemented | `merge.*`, `merge.wasm.*`, conflict handling paths. |
| R08 Incremental views | Hardened | `view.create/drop/refresh/status/explain_deps`; BFS-based cascading invalidation through view-on-view deps; `view_dependency_graph()` with `ViewDepEdge` structs; transitive dep traversal in `view_explain_deps`. |
| R09 QUIC-native protocol | Implemented (prototype) | QUIC transport + integration tests (`tests/quic_rpc.rs`). |
| R10 Vector embeddings | Hardened | `vector.insert/search/index.status`; HNSW graph index (M=16, M_max0=32, ef_construction=64) with insert/search/prune; automatic index rebuild on insert; cosine/dot/L2 metrics; falls back to brute-force when filter is active. |
| R11 LLM/autoparam | Prototype implemented | `ai.autoparam.classify/analyze`. |
| R12 NL -> SkeinQL | Prototype implemented | `ai.nl.translate/explain/execute`. |
| R13 Causal ETag consistency | Hardened | ETag/min-causality controls; V2 vector-clock format (`CAUSALITY_FORMAT_V2`); `merge_causality_tokens()` (component-wise max); `causality_dominates()` (partial order); `ensure_min_causality()` accepts V1+V2. |
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
