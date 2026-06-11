# SkeinDB True Status Matrix

Last updated: 2026-06-11

This is the short truth surface. It is intentionally not a changelog. Use it to answer: what is real today, what is partial, and what should not be claimed yet.

## Current Truth Snapshot

- **Compatibility:** SkeinDB does **not** claim full MySQL or PostgreSQL compatibility. MySQL coverage is broad and corpus-backed; PostgreSQL support is a partial PG v3 baseline. See [docs/MYSQL_COMPAT.md](./MYSQL_COMPAT.md) and [docs/PG_COMPAT.md](./PG_COMPAT.md).
- **Core roadmap:** [docs/PROJECT_BACKLOG.md](./PROJECT_BACKLOG.md) has **140 done / 0 open** top-level roadmap checkboxes. That does not mean every phase is production-complete; several phases remain partial or prototype-strength.
- **Research roadmap:** [docs/RESEARCH_BACKLOG.md](./RESEARCH_BACKLOG.md) has **109 done / 0 open** research checkboxes. R01-R17 and R20 are hardened; R18 and R19 remain prototype implemented.
- **Latest verified slice:** PostgreSQL CSV `COPY` now honors single-byte `ESCAPE` markers in addition to `QUOTE` across export and import on the live PG path, while the recent catalog parity work still includes `pg_catalog.pg_description` alongside `pg_catalog.pg_am`.

## Do Not Overclaim

- No "100% MySQL compatibility" or "100% PostgreSQL compatibility" claim.
- No production-complete LSM/MANIFEST/WAL storage claim for the whole engine.
- No claim that CDC has external sinks, cluster-wide fanout, broader predicates, or binary/columnar event encodings.
- No claim that R18 performance replay or R19 Wasm query operators are hardened.
- No claim that distribution packaging is fully published until signing/release secrets are configured.

## Current Partial Areas

| Area | Truth today | Remaining gap |
|---|---|---|
| Storage core | JSON/segment prototype paths + exercised core primitives (Manifest with typed replay, WAL with committed recovery + torn-tail truncation, RowSeg/RV1 + FilePtr MVCC chains, ValueStore + DELTA + learned indexes, Run/LSM blocks, rowdir, encryption envelopes, Wasm catalog/UDF sandbox) with extensive unit + integration + checkpoint + replay + restart tests exist. Code locations: engine.rs:1972 (open_with_storage_mode + load_tables_best_effort + rebuild_value_store_from_tables_best_effort), 2383+ (persist_table everywhere), core/src/{manifest.rs,wal.rs,rowseg.rs,valuestore.rs,run.rs, rowdir.rs,mvcc.rs,encrypted_valuestore.rs}. | Full production MANIFEST/WAL/LSM pipeline as the primary always-on row persistence path (current engine still relies on hybrid/segment snapshotting + full in-memory table materialization) remains partial. No streaming/large table support; crash safety between persists is best-effort. |
| PostgreSQL compatibility | PG listener, startup/auth, many SQL rewrites, catalog probes (pg_database, pg_namespace, pg_am, pg_description, pg_proc, pg_type, pg_settings, pg_stat_activity etc via virtual tables), COPY text/csv/binary with `NULL` / `HEADER` / `HEADER MATCH` / `DELIMITER` / `QUOTE` / `ESCAPE`, parenthesized and legacy bare `WITH CSV|TEXT|BINARY` format aliases, extended query (Parse/Bind/Execute), binary params for common OIDs, SQLSTATE mapping, and corpus coverage exist. Code: pg_wire.rs (framing, auth, COPY encode/decode), server.rs PG paths + virtual catalog, cluster_rpc.rs pg_*_roundtrip tests, tests/compat/pg_corpus.sql. | Not full PostgreSQL; broader COPY option coverage beyond current (e.g. more formats, OIDs, portal suspension), wider catalog parity (more pg_* tables), production driver matrices (Django, Rails, full psycopg etc.), broader dialect. |
| CDC/changefeeds | Local table/query subscriptions work with polling, SSE, WebSocket, durable cursors, row images, `objects_json`/`plain_json`, op/pk/range/column filters, pause/resume, backpressure, and resnapshot signaling. Dependency expansion over views/CTE/union/set-ops. External sink config struct exists. Code: engine.rs cdc_* (event delivery, project, subscriptions v9), server.rs CDC RPC + sink drain. Tests in cluster_rpc and engine. | Broader predicates, binary/columnar encodings, external sink connectors implementation, cluster-wide fanout remain. |
| Compaction scheduler | Live policy/status controls, safe-mode write backpressure, and a pressure-driven worker exist. | Deeper multi-level/file-count compaction across many live tables remains future storage-engine work. |
| Distribution | Debian metadata, Homebrew formula, and tag-driven release workflow scaffolding exist. | Signed apt publication depends on configured secrets and release operations. |
| R18 performance replay | Replay bundles can carry performance profiles and run variance checks. | Timing injection, cache/LSM reconstruction fidelity, and CI distribution gates remain open. |
| R19 Wasm query operators | Compile/inspect/run/perf/edge-package surfaces exist with generated fixed-width Wasm artifacts and host fallback. | Production SIMD-lowered codegen and hardened operator breadth are not claimed. |

## Recent Verified Changes

- **2026-06-11:** Full workspace verification + small storage hardening: added `Engine::core_lsm_files_active()` + `EngineStorageStats.core_lsm_active` (engine.rs) with test evidence in dedup_stats_roundtrip_across_restart. Confirmed core LSM files (MANIFEST + wal-*.log) for non-JSON modes. Matrix Storage core / Phase 1 gap description + evidence updated with code locations and this observability step toward primary pipeline.
- **2026-05-28:** PostgreSQL CSV `COPY` now honors single-byte `ESCAPE` markers on supported CSV forms, allowing custom quote and escape characters to round-trip values containing both characters over `COPY ... TO STDOUT` and `COPY ... FROM STDIN`. Coverage: `server::tests::pg_copy_csv_encode_and_parse_rows_handle_custom_escape_marker`, `server::tests::pg_parse_copy_to_stdout_recognizes_table_and_query_sources`, `server::tests::pg_parse_copy_from_stdin_and_decode_rows`, `cluster_rpc.rs::pg_simple_query_copy_to_stdout_with_csv_escape_roundtrip`, `cluster_rpc.rs::pg_simple_query_copy_from_stdin_with_csv_escape_roundtrip`.
- **2026-05-28:** PostgreSQL catalog parity now includes `pg_catalog.pg_description` as an empty but correctly typed virtual table, so `SELECT * FROM pg_catalog.pg_description` returns PostgreSQL-shaped row descriptions and zero rows over both `sql.exec` and the live PG listener. Coverage: `server::tests::sql_exec_pg_catalog_virtual_tables_roundtrip`, `cluster_rpc.rs::pg_simple_query_pg_catalog_virtual_tables_roundtrip`.
- **2026-05-28:** PostgreSQL CSV `COPY` now honors single-byte `QUOTE` markers across export and import on supported CSV forms, including round-tripping quoted delimiters, empty strings, and doubled custom quote characters. Coverage: `server::tests::pg_copy_csv_encode_and_parse_rows_handle_custom_quote_marker`, `server::tests::pg_parse_copy_to_stdout_recognizes_table_and_query_sources`, `server::tests::pg_parse_copy_from_stdin_and_decode_rows`, `cluster_rpc.rs::pg_simple_query_copy_to_stdout_with_csv_quote_roundtrip`, `cluster_rpc.rs::pg_simple_query_copy_from_stdin_with_csv_quote_roundtrip`.
- **2026-05-28:** PostgreSQL catalog parity now includes `pg_catalog.pg_am`, exposing `heap` and `btree` access-method rows with stable OIDs aligned to the current `pg_class.relam` values over both `sql.exec` and the live PG listener. Coverage: `server::tests::sql_exec_pg_catalog_virtual_tables_roundtrip`, `cluster_rpc.rs::pg_simple_query_pg_catalog_virtual_tables_roundtrip`.
- **2026-05-28:** PostgreSQL text/csv `COPY` now honors custom `NULL '...'` markers across export and import, quotes literal `"NULL"` CSV cells so they round-trip distinctly from unquoted nulls, and enforces CSV `HEADER MATCH` on `COPY ... FROM STDIN` by rejecting mismatched header names before insert assembly. Coverage: `server::tests::pg_copy_text_encode_and_parse_rows_handle_custom_null_marker`, `server::tests::pg_copy_csv_encode_and_parse_rows_handle_custom_null_marker`, `server::tests::pg_parse_copy_to_stdout_recognizes_table_and_query_sources`, `server::tests::pg_parse_copy_from_stdin_and_decode_rows`, `cluster_rpc.rs::pg_simple_query_copy_csv_with_custom_null_string_roundtrip`, `cluster_rpc.rs::pg_extended_query_copy_csv_with_custom_null_string_roundtrip`, `cluster_rpc.rs::pg_simple_query_copy_from_stdin_with_csv_header_match_rejects_mismatch`.
- **2026-05-28:** PostgreSQL `COPY` now accepts PostgreSQL-style `WITH (CSV|TEXT|BINARY)` format aliases inside COPY option lists, and the live PG harness now explicitly covers extended-query CSV `HEADER MATCH` mismatch rejection. Coverage: `server::tests::pg_parse_copy_to_stdout_recognizes_table_and_query_sources`, `server::tests::pg_parse_copy_from_stdin_and_decode_rows`, `cluster_rpc.rs::pg_simple_query_copy_to_stdout_with_csv_keyword_format_alias_roundtrip`, `cluster_rpc.rs::pg_extended_query_copy_from_stdin_with_csv_header_match_rejects_mismatch`.
- **2026-05-28:** PostgreSQL `COPY` now also accepts legacy bare `WITH CSV|TEXT|BINARY` syntax, and the live PG harness covers keyword-alias copy-in roundtrips in addition to copy-out. Coverage: `server::tests::pg_parse_copy_to_stdout_recognizes_table_and_query_sources`, `server::tests::pg_parse_copy_from_stdin_and_decode_rows`, `cluster_rpc.rs::pg_simple_query_copy_from_stdin_with_csv_keyword_format_alias_roundtrip`, `cluster_rpc.rs::pg_simple_query_copy_to_stdout_with_legacy_with_csv_header_roundtrip`.
- **2026-06-01:** PostgreSQL binary `COPY ... FROM STDIN` now round-trips over both simple-query and extended-query flows, decoding the standard PostgreSQL binary stream (signature, flags, tuple framing, per-field values) and committing rows through the shared write path. Coverage: `server::tests::pg_parse_copy_from_stdin_and_decode_rows`, `cluster_rpc.rs::pg_simple_query_copy_from_stdin_with_binary_format_roundtrip`, `cluster_rpc.rs::pg_extended_query_copy_from_stdin_with_binary_format_roundtrip`.
- **2026-05-27:** PostgreSQL `split_part(text, delimiter, n)` now works through the shared evaluator with positive and negative field indexes, returns `text` metadata on the PG wire path, and is listed in `pg_catalog.pg_proc`. Coverage: `engine::tests::eval_pg_split_part`, `server::tests::sql_exec_pg_catalog_virtual_tables_roundtrip`, `cluster_rpc.rs::pg_simple_query_split_part_roundtrip`.
- **2026-05-27:** `stats.snapshot.alerts` now supports settings-backed route matching plus `stats.snapshot`-driven HTTP(S) webhook delivery for newly active matched alerts, exposing per-alert route delivery counters and top-level `routing.delivery.{delivered,suppressed,failed,unsupported}` metadata. Coverage: `server::tests::stats_snapshot_routes_operator_alerts_from_settings`, `server::tests::stats_snapshot_delivers_http_alert_routes_once_per_active_alert`.
- **2026-05-27:** encryption profile metadata and the redacted audit ring now persist across reopen in `data/encryption.json`, preserving `mode` / `active_key_id` status without persisting master keys. Coverage: `engine::tests::encryption_status_persists_profiles_and_audit_across_restart`.
- **2026-05-29:** encrypted-at-rest cell payloads are now wired through the engine row-persistence path. When a database has an active encryption profile (`ENC_RANDOM`/`ENC_MLE_DB`), encryptable cells (`Str`/`Json`/`Bytes`/`Uuid`/`Embedding`) are stored as `"$skein_enc"` envelopes in table-row files (`format_version` bumped 3→4), scalar/key cells stay plaintext, and `ENC_MLE_DB` stays deterministic for equal plaintext. Master keys are still never persisted: tables loaded without their key are marked locked (zero rows, persist refused so ciphertext is preserved) and transparently reload/unlock when the key is registered. Coverage: `engine::tests::encrypted_at_rest_cells_roundtrip_and_lock_without_key`, `engine::tests::encrypted_at_rest_mle_is_deterministic_for_equal_plaintext`. See `docs/ON_DISK_FORMAT.md` §11.8.1.
- **2026-05-27:** `advisor.evaluate` latency benchmarking now covers join-key samples, multi-range ordered samples, multi-column grouped workloads, and grouped range+order workloads. Coverage: `engine::tests::advisor_evaluate_reports_latency_benchmark_for_join_key_workload`, `engine::tests::advisor_evaluate_reports_latency_benchmark_for_multi_range_order_workload`, `engine::tests::advisor_evaluate_reports_latency_benchmark_for_multi_group_workload`, `engine::tests::advisor_evaluate_reports_latency_benchmark_for_range_group_order_workload`, `cluster_rpc.rs::r16_index_advisor_evaluate_reports_join_key_latency_benchmark`, `cluster_rpc.rs::r16_index_advisor_evaluate_reports_multi_range_order_latency_benchmark`, `cluster_rpc.rs::r16_index_advisor_evaluate_reports_multi_group_latency_benchmark`, `cluster_rpc.rs::r16_index_advisor_evaluate_reports_range_group_order_latency_benchmark`.
- **2026-05-27:** CDC `plain_json` format added and persisted in `cdc_subscriptions.json` format v8. Coverage: `engine::tests::cdc_table_subscription_plain_json_format_persists_and_serializes`, `server::tests::cdc_table_subscription_plain_json_format_roundtrip`, `cluster_rpc.rs::cdc_table_subscription_plain_json_format_roundtrip`.
- **2026-05-27:** CDC query dependency extraction now handles CTE definitions while ignoring CTE aliases as physical tables. Coverage: `engine::tests::cdc_query_subscription_over_cte_query_invalidates_on_base_table_changes`, `cluster_rpc.rs::query_subscribe_over_cte_reports_base_table_keys_and_emits_sse_on_base_changes`.
- **2026-05-27:** CDC query dependency extraction handles set-operation branches and view-expanded base tables. Coverage includes the `cdc_query_subscription_over_union...` and `cdc_query_subscription_over_view...` engine tests plus matching `cluster_rpc.rs` SSE tests.
- **2026-05-27:** CDC primary-key range filters are supported on single-column primary keys and persisted in subscription state.
- **2026-05-26:** CDC source-op, exact primary-key, and changed-column filters are live for table and query subscriptions.

## Core Roadmap Status

| Phase | Current status | Short truth |
|---|---|---|
| Phase 0 Repo setup | Implemented | Primitive file/record/value-id building blocks have runtime tests. |
| Phase 1 Storage core | Partial | Prototype persistence exists; full production storage pipeline is not complete. |
| Phase 2 SQL + metadata | Partial | Catalog, DDL/DML subset, and compatibility metadata exist. |
| Phase 3 MySQL protocol | Implemented baseline | MySQL listener and broad SQL compatibility corpus pass; not full MySQL. |
| Phase 4 Web console | Partial advanced | SkeinAdmin is embedded with broad live panels; console remains evolving. |
| Phase 5 SkeinQL API | Implemented baseline | Typed RPC/API surface exists for core operations. |
| Phase 6 ETag cache coherence | Implemented baseline | ETags, prepared GET, and dependency notifications exist. |
| Phase 7 Delta values | Implemented prototype | Delta storage behavior is covered by ValueStore tests. |
| Phase 8 Wasm extensions | Implemented baseline | Wasm catalog/UDF sandbox exists; query operators are R19 prototype. |
| Phase 9 Audit WAL | Implemented baseline | Forensic hash chain, anchors, verify/status, and export tooling exist. |
| Phase 10 Row/column snapshots | Partial | Snapshot build/read/optimizer coverage exists for selected shapes. |
| Phase 11 Compat telemetry + migration | Implemented baseline | Telemetry and migration intent/rewrite/report surfaces exist. |
| Phase 12 SkeinAdmin | Implemented baseline | Admin UI covers major runtime surfaces and research panels. |
| Phase 13 Observability | Partial advanced | Stats, metrics, latency, CDC telemetry, basic alerts, settings-backed alert routing, and `stats.snapshot`-driven HTTP(S) webhook delivery exist; standalone escalation automation remains. |
| Phase 14 Cluster scale-out | Implemented baseline | Node identity, replication, shard movement, CAS transfer, and routing hints exist. |
| Phase 15 Perf improvements | Implemented baseline | Interning, late materialization, batch scan, and MVCC visibility cache exist. |
| Phase 16 Query coalescing | Implemented baseline | Coalescing is live for prepared GET and patch paths. |
| Phase 17 CAS-aware replication | Implemented baseline | Object need/missing/fetch/pull and shard-manifest transfer exist. |
| Phase 18 Index advisor | Partial advanced | Advisor synthesis/apply/retire/evaluate exists; non-grouped range+order layouts without the same leading key still fall back. |
| Phase 19 Time travel + replay | Implemented baseline | MVCC `as_of`, history, replay export/import/run, and admin tooling exist. |
| Phase 20 Encryption | Partial baseline | Crypto primitives, envelope helpers, rotation helpers, settings controls, and persisted mode/active-key/audit metadata exist. Engine row persistence now writes encrypted-at-rest cell envelopes (`format_version` 4, `$skein_enc`) for encryptable cell kinds when a DB profile is active, with deterministic `ENC_MLE_DB` and a locked-table guard when keys are absent. Master key bytes are still not persisted, scalar/key cells remain plaintext. Code: core/encrypted_valuestore.rs, core/encryption.rs, engine.rs encryption_* + key_manager. Core tests + engine integration tests. | secondary value-store path is not yet routed through encrypted storage. Full ValueStore secondary + complete primary row path coverage still partial. |
| Phase 21 Compaction scheduler | Partial | Live scheduler controls and worker exist; deeper storage compaction remains. |
| Phase 22 Autoparam + plan cache | Implemented baseline | Autoparam classifiers/feedback/metrics and plan-cache controls exist. |
| Phase 23 CDC/changefeeds | Partial | Local CDC is strong; external sinks/fanout and richer encodings remain. |
| Phase 25 PostgreSQL compat | Partial advanced | PG v3 baseline is substantial but not full PostgreSQL. |
| Phase 26 Distribution | Partial | Packaging scaffolding exists; signed publication requires release configuration. |

## Research Track Status

| Track | Status | Primary runtime surface |
|---|---|---|
| R01 Learned indexes | Hardened | ValueStore learned-index reports, lookup traces, refresh policy, and benchmark probes. |
| R02 Adaptive row/column | Hardened | Snapshot optimizer, adaptive replacement, dependency refresh, and hybrid execution scaffolds. |
| R03 Delta topology | Hardened | Delta-chain policy, skip patches, compaction, and topology analysis. |
| R04 Differential privacy | Hardened | `dp.*` aggregates, budgets, audit, accuracy evaluation, and RDP composition. |
| R05 Oblivious execution | Hardened | `oblivious.policy.*`, padded execution, explain/evaluate, and privacy controls. |
| R06 Forensic WAL queries | Hardened | `forensic.query`, `forensic.verify`, `forensic.export`, Merkle proofs, and bundles. |
| R07 Client-side merge funcs | Hardened | `merge.*`, Wasm merge policies, offline queue spec, and conflict evaluation. |
| R08 Incremental views | Hardened | `view.*` create/refresh/evaluate/status/explain_deps with dependency tracking. |
| R09 QUIC-native protocol | Hardened | QUIC RPC transport, 0-RTT write rejection, migration/rebind, and transport benchmark. |
| R10 Vector embeddings | Hardened | `vector.insert/search/benchmark/index.status`, HNSW/LSH, cache metadata, and RAG sample. |
| R11 LLM/autoparam | Hardened | `ai.autoparam.*` classifier catalog, labels, feedback, and metrics. |
| R12 NL -> SkeinQL | Hardened | `ai.nl.translate/explain/execute`, approval tokens, and eval harness. |
| R13 Causal ETag consistency | Hardened | `min_causality`, vector clocks, causal validators, and replication watermarks. |
| R14 Replay bundles | Hardened | `edge.bundle.*` plus replay bundle redaction/export/import/run. |
| R15 Schema evolution | Hardened | `schema.propose_change/merge_status/simulate_rollout/apply_merge`. |
| R16 Auto index synthesis | Hardened | `advisor.*` synthesize/apply/retire/evaluate/history/dismiss. |
| R17 Intent inference | Hardened | `migration.intent_report`, `migration.rewrite_preview`, and `migration.report_export`. |
| R18 Perf regression replay | Prototype implemented | Performance profiles and replay variance exist; fidelity/CI gates remain open. |
| R19 Wasm query operators | Prototype implemented | Wasm plan compile/inspect/run/perf/edge-package exists; production SIMD breadth remains open. |
| R20 Energy-aware compaction | Hardened | Energy-aware compaction policy, external energy signals, status/stats, and eval harness. |

## Compatibility Guardrail

SkeinDB coverage is measured against `tests/compat/corpus.sql` and live integration tests. If marketing copy says "100% MySQL compatibility" or "100% PostgreSQL compatibility," treat that as false until this file and the compatibility docs explicitly say otherwise.

## Truth-Maintenance Rule

When a task is promoted from prototype to hardened behavior, update:

1. The corresponding entry in [docs/PROJECT_BACKLOG.md](./PROJECT_BACKLOG.md) or [docs/RESEARCH_BACKLOG.md](./RESEARCH_BACKLOG.md).
2. This matrix row, including any remaining gap.
3. At least one test reference proving the claim.
