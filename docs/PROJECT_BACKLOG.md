# SkeinDB Project Backlog

This backlog is designed for small PR-sized tasks.
Each task should include tests.

## Reality sync (2026-03-30)

This file now reflects a stricter distinction:
- `[x]` = implemented and exercised in runtime/tests.
- `[ ]` = still open (including prototype/partial work that still needs hardening).

For the full implemented-vs-partial matrix (core + research), see:
- `docs/TRUE_STATUS_MATRIX.md`

## Phase 0 — Repo setup
- Status: complete in runtime + tests (`crates/skeindb-core/src/lib.rs`, `crates/skeindb-core/tests/phase0_format.rs`)
- [x] T001: Encoding primitives (VarU, Bytes/String, CRC32C)
- [x] T002: FileHeader read/write
- [x] T003: RecordFrame append/iterate

Phase 0 verification checklist:
- [x] T001 evidence: VarU and hash/CRC tests (`tests::varu_roundtrip_*`, `tests::value_id_is_stable`, `tests::audit_hash_is_stable`) in `crates/skeindb-core/src/lib.rs`
- [x] T002 evidence: FileHeader encode/decode + corruption tests in `crates/skeindb-core/src/lib.rs` and file roundtrip in `crates/skeindb-core/tests/phase0_format.rs`
- [x] T003 evidence: RecordFrame append/decode/iterate + truncation/CRC tests in `crates/skeindb-core/src/lib.rs` and file-backed iteration in `crates/skeindb-core/tests/phase0_format.rs`

## Phase 1 — Storage core
- [ ] T010: MANIFEST.log reader/writer
- [ ] T011: WAL writer/reader + recovery
- [ ] T012: ValueStore (.vseg) append/read + ValueID
- [ ] T013: Sorted runs (.run) + simple LSM (memtable + level0)
- [ ] T014: RowSeg (.rseg) + RowVersion encoding
- [ ] T015: RowDir (row_id -> head ptr)
- [ ] T016: MVCC visibility

## Phase 2 — SQL + virtual metadata
- [x] T020: Catalog schema + TableDef
- [x] T021: information_schema.tables + columns
- [x] T022: Minimal executor: CREATE TABLE, INSERT, SELECT scan+filter+limit

## Phase 3 — MySQL protocol
- Status: baseline protocol plus broad COM_QUERY / COM_STMT compatibility are implemented in runtime/tests; follow-up parity work now continues through corpus growth and later backlog phases rather than open Phase 3 checkboxes.
- [x] T030: Handshake + mysql_native_password
- [x] T031: COM_QUERY SELECT literals
- [x] T032: SQL translator (subset)
- [x] T033: DDL/DML subset for corpus.sql
- [x] T034: SQL_CALC_FOUND_ROWS + FOUND_ROWS
- [x] T035: Index-backed secondary/unique index enforcement for MySQL duplicate-key semantics (runtime duplicate-key checks now reuse the shared secondary-index cache, including `PRIMARY KEY`-changing `UPDATE`s; MySQL duplicate-key failures now surface as `1062` / `23000` on the wire; creating a MySQL compatibility `UNIQUE INDEX` still rejects pre-existing duplicate rows; and per-table secondary-index cache metadata now persists/reloads on reopen)
- [x] T036: Broaden COM_QUERY parity for WordPress-class workloads (the MySQL listener now covers the checked-in WordPress-style corpus and companion integration tests, including grouped/simple aggregate shims with `HAVING`, projection-grouped `GROUP BY` de-dup including wildcard projections after expansion, `SQL_CALC_FOUND_ROWS`, wildcard join projections, top-level comma joins and left-associative join chains, parenthesized boolean predicates, index DDL, bootstrap/session compatibility `SET` and `SHOW` forms, recursive/nested compatibility rewrites for the current subquery subset, and compatibility no-op `LOCK TABLES` / `UNLOCK TABLES`)
- [x] T037: Deepen COM_STMT parity beyond the current baseline (complex-query result metadata, stricter driver/cursor semantics, fuller protocol coverage; prepare-time metadata now also covers supported scalar-expression projections including baseline arithmetic, broader scalar/date-time functions including `FIND_IN_SET` / `ISNULL`, `DATE_FORMAT` / `FROM_UNIXTIME`, `DATEDIFF` / `TIMESTAMPDIFF`, `WEEKDAY` / `DAYOFWEEK` / `DAYOFYEAR`, `MONTHNAME` / `DAYNAME`, `QUARTER`, `LAST_DAY`, `EXTRACT(<unit> FROM ...)`, and baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` / `TIMESTAMPADD`, supported subquery-compat `SELECT`s whose `WHERE` clauses rewrite cleanly, including the current `IN` / `EXISTS` / simple scalar-compare subset, the current nested compatibility path, limited negated boolean-tree wrappers when they can still rewrite cleanly, supported projection-level scalar subqueries, embedded scalar-subquery arithmetic, plus `CASE` / `CAST` plus simple aggregate / grouped-aggregate compatibility queries). Progress: the new scalar/date-time functions, `COM_INIT_DB`, and `COM_STATISTICS` wire commands broaden the prepared-statement surface, and the latest slice adds dedicated unit + MySQL-wire regressions for projection-subquery metadata parity.
- [x] T038: Broaden COM_QUERY beyond the current WordPress-class baseline (deeper correlated/nested subqueries beyond the current recursive `IN` / `EXISTS` / simple scalar-compare compatibility path, broader join parity beyond the current left-associative `ON` plus simple base-table `USING` subset, broader date/time/function parity beyond the current scalar/date-time baseline, and broader `ALTER TABLE` variants beyond the current `ADD/MODIFY/CHANGE/RENAME COLUMN/RENAME [KEY|INDEX]/RENAME TO/DROP COLUMN` plus index metadata surface). Progress: significant surface expansion — added `BETWEEN`/`NOT BETWEEN`, `COUNT(DISTINCT col)`, `GROUP_CONCAT()`, `INSERT ... SELECT`, `UNION`/`UNION ALL`, `TRUNCATE TABLE`, `DROP DATABASE`, `RENAME TABLE`, `EXPLAIN` stub, `DO`, `SAVEPOINT` stubs, `CREATE VIEW`/`DROP VIEW` stubs, locking hint stripping, session functions (`USER()`, `LAST_INSERT_ID()`, `CONNECTION_ID()`), `information_schema.schemata`/`statistics`, expanded `SHOW` commands (WARNINGS, ERRORS, PROCESSLIST, TRIGGERS, EVENTS, PROCEDURE STATUS, FUNCTION STATUS, PLUGINS, PROFILES, CREATE DATABASE), `SET GLOBAL`/`FLUSH`/`ANALYZE`/`OPTIMIZE`/`CHECK`/`REPAIR`/`KILL` no-ops, and 30+ additional scalar/date-time functions. Corpus expanded from 772→947 lines (283 statements). Latest batch: derived tables (FROM subqueries), CTEs (`WITH...AS`), `REGEXP`/`RLIKE`/`NOT REGEXP`, `<=>` (NULL-safe equality), `NATURAL JOIN`, `FULL OUTER JOIN` (fully executed), multi-table `DELETE`, multi-table `UPDATE` (stub), 11 JSON functions (`JSON_EXTRACT`, `JSON_UNQUOTE`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_CONTAINS`, `JSON_LENGTH`, `JSON_TYPE`, `JSON_VALID`, `JSON_SET`, `JSON_KEYS`, `JSON_MERGE_PRESERVE`), plus `FIELD`/`ELT`, `INET_ATON`/`INET_NTOA`, `BIN`/`OCT`/`CONV`, and hash functions (`CRC32`, `MD5`, `SHA1`/`SHA`, `SHA2`). Corpus now at 1130 lines (over 374 statements). Latest batch: multi-column `GROUP BY` with multiple group columns and aggregates, 12 new scalar functions (`SUBSTRING_INDEX`, `ASCII`, `ORD`, `CHAR`, `STRCMP`, `BIT_LENGTH`, `OCTET_LENGTH`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `TO_BASE64`, `FROM_BASE64`), 5 new `information_schema` stub tables (`routines`, `triggers`, `views`, `processlist`, `user_privileges`). Latest batch: window functions (`ROW_NUMBER()`/`RANK()`/`DENSE_RANK()` with `OVER(PARTITION BY ... ORDER BY ...)`), `SET @var = ...`/`SELECT @var` user variables, `BIT_AND()`/`BIT_OR()`/`BIT_XOR()` bitwise aggregates, multi-table `UPDATE` (upgraded from stub to real per-row implementation), 6 new scalar functions (`DEGREES`, `RADIANS`, `PERIOD_ADD`, `PERIOD_DIFF`, `MAKEDATE`, `MAKETIME`). Corpus now at 1240+ lines (over 370 statements). Latest batch: corpus.sql fully expanded — all 16+ TODO blocks uncommented (IF/NULLIF, EXISTS, REGEXP, CAST, COUNT DISTINCT, INFORMATION_SCHEMA, LOCK/UNLOCK, window functions, CTEs, RIGHT/CROSS JOIN, derived tables, NOT EXISTS, IN/scalar subquery, multi-table DELETE/UPDATE, nested functions, SHOW PROCESSLIST/PLUGINS). ~60 new SQL statements added covering additional JOINs, INSERT...SELECT, DO, EXPLAIN, SHOW variants, system variables (@@version etc.), maintenance no-ops, CREATE/DROP VIEW, SAVEPOINT, GROUP_CONCAT DISTINCT, multi-column GROUP BY, session functions, locking hints, scalar functions, SET GLOBAL. Corpus now at 1657 lines (about 678 semicolon-terminated SQL statements) after the fully expanded compatibility sweep. Latest batch: correlated subqueries in projection (`SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) FROM users`), binary comparison operators in scalar expressions (`>`, `<`, `>=`, `<=`, `=`, `!=`, `<>`), multi-aggregate GROUP BY with ORDER BY support over JOINs, embedded subquery pre-evaluation in arithmetic expressions (`salary - (SELECT AVG(salary) FROM users)`), expression-based UPDATE SET values with per-row evaluation (`UPDATE users SET salary = salary * 1.1 WHERE ...` via `data_update_exprs` engine method), WordPress Site Health-style `information_schema.TABLES` storage summaries, WordPress Users-screen role counts via `COUNT(NULLIF(<predicate>, false))`, and dedicated MySQL-wire regressions for WordPress installer/admin seed queries. A fresh live WordPress admin sweep across the core dashboard/content/settings surfaces now finishes with an empty `debug.log`; only the theme-owned `nav-menus` / `widgets` pages still return non-database 500s. Deeper parity work is still ongoing.

## Phase 4 — Web console
- [x] T040: HTTP API `/api/v1/sql/exec`
- [x] T041: Console UI scaffold
- [x] T042: Schema browser + SQL editor
- [x] T043: Data browse/edit + import/export (CSV + JSON export/import)
- [x] T044: Users/privileges + status dashboard

## Phase 5 - SkeinQL native API
- [x] T050: Define SkeinQL request/response types + error model (docs/SKEINQL.md)
- [x] T051: Implement HTTP RPC endpoint POST /api/v1/rpc (system.ping, system.version)
- [x] T052: Implement schema.* methods (list/describe/create/drop)
- [x] T053: Implement query.select (single-table scan + filter + limit) over SkeinIR
- [x] T054: Implement tx.begin/commit/rollback via SkeinQL

## Phase 6 - Cache-coherent HTTP queries (ETags)
- [x] T060: Row ETags for data.get and If-Match support for data.update
- [x] T061: Planner dependency sets for simple indexed queries
- [x] T062: query.prepare + GET /api/v1/q/{query_id} with ETag/If-None-Match
- [x] T063: SSE subscription to ETag changes (query.subscribe)

## Phase 7 - Delta-chained values
- [x] T070: Add ValueEntry kind DELTA + patch codec (docs/DELTA_VALUES.md)
- [x] T071: Delta selection policy + metrics
- [x] T072: Compaction rebase (limit delta chain depth)

## Phase 8 - Wasm extensions
- [ ] T080: Module store + catalog metadata for UDFs (docs/WASM_UDFS.md)
- [ ] T081: Scalar UDF execution sandbox with resource limits
- [ ] T082: Safe cancellation (fuel/time budget) + tests
- [ ] T083: Aggregate and table-function UDFs

## Phase 9 - Tamper-evident WAL audit
- [x] T090: WALHeader v2 with hash chaining (docs/AUDIT_WAL.md)
- [x] T091: checkpoint anchors + audit status
- [x] T092: audit verify CLI/API + console page. Latest: SkeinAdmin's Forensics panel now exposes `maintenance.audit_status` and `maintenance.audit_verify` alongside the prototype `forensic.query` / `forensic.verify` / `forensic.export` tools.

## Phase 10 - Hybrid row/column snapshots
- [ ] T100: Snapshot builder (scan MVCC at snapshot_ts) + cseg writer (docs/COLUMN_SNAPSHOTS.md)
- [ ] T101: Snapshot reader + column scan operator
- [ ] T102: Optimizer rule: use column snapshots for covered ranges

## Phase 11 - Compatibility telemetry and migration hints
- [x] T110: Feature flag instrumentation in MySQL translator
- [x] T111: Internal storage for telemetry counters + query fingerprints (optional)
- [x] T112: telemetry.compat_summary endpoint + console dashboard
- [x] T113: telemetry.migration_hints generator (MySQL patterns -> SkeinQL calls)

## Phase 12 - Standalone management console (SkeinAdmin)
- [x] T120: SkeinAdmin placeholder scaffold (web/skeinadmin) + connection profiles
- [x] T121: SkeinAdmin pages: schema/data/sql workspace
- [x] T122: SkeinAdmin security: token UI + role-aware navigation. Latest: dedicated Security panel remains reachable from both sidebar and top-tab navigation, with create/list/revoke token flows using modal confirmations instead of browser dialogs.
- [x] T123: SkeinAdmin cluster page (cluster.*) + actions. Latest: join/leave/remove/promote controls are all surfaced in the live cluster panel.
- [x] T124: SkeinAdmin observability page (stats.*) — comprehensive dashboard with runtime, storage/dedup, MVCC/compaction, query/cache stats + auto-refresh
- [x] T125: SkeinAdmin Easy Viewer (phpMyAdmin-inspired) — sidebar tree, sub-tabs, inline editing, search, export, operations. Latest: inline New DB flow, live create-table SQL preview, duplicate-column / identifier validation before create, required-field validation before insert, column sorting (click-to-sort headers), styled modal confirmations (replacing browser confirm()), search operator dropdown (LIKE/=/!=/>/</BETWEEN/IS NULL/IS NOT NULL/REGEXP), visual query builder tab (column picker, WHERE condition builder, ORDER BY/LIMIT, SQL preview, execute/copy/send), 5 new dashboard cards (Top Tables, Slow Query Log, Active Sessions, Index Health, Research Track Status)
- [x] T126: SkeinAdmin Engine Config panel — checkbox toggles for dedup, compression, encryption, MVCC, delta chains, time travel, compaction, cache, security, replication, CDC, QUIC. Latest: storage mode selector is aligned with the real runtime values `json`, `segment`, and `hybrid`.

## Phase 13 - Observability and server load statistics
- [x] T130: stats.snapshot and basic counters in server
- [x] T131: query fingerprinting + top_queries / slow_queries
- [x] T132: GET /metrics (Prometheus-style) + labels
- [x] T133: Console widgets for CPU/memory/disk/QPS/TPS/compaction — Overview dashboard with stat cards, dedup bar chart, auto-refresh

## Phase 14 - Cluster management and scale-out
- [x] T140: Node identity (node_id) + cluster config model
- [x] T141: Replication transport protocol (primary -> replica fanout over SkeinQL RPC)
- [x] T142: CAS object pull protocol (replica fetch missing ValueIDs; objects.need/missing/fetch RPCs + Bloom contains)
- [x] T143: Read-only replica serving + router (cluster.route_query RPC + replica write rejection)
- [x] T144: cluster.* SkeinQL endpoints + join tokens + promote replica
- [x] T145: Sharding metadata + router prototype (single-shard txns)

## Phase 15 - Additional performance improvements
- [ ] T150: Schema flag for interned columns + ValueID-first predicate ops (docs/PERFORMANCE.md)
- [ ] T151: Late materialization (decode only projected columns)
- [ ] T152: Batch (vectorized) scan/filter/project pipeline
- [ ] T153: MVCC Visible Version Index cache

## Phase 16 - Query coalescing (thundering herd protection)
- [x] T160: Query fingerprint canonicalization (SkeinIR + SkeinQL) + auth scope keying
- [x] T161: In-flight query map (leader/joiner) with cancellation semantics
- [x] T162: Enable coalescing for GET /api/v1/q/{query_id} (cacheable) + tests
- [x] T163: Metrics + limits + SkeinAdmin dashboard widget

## Phase 17 - CAS-aware replication bandwidth bounds (object-aware sync)
- [x] T165: Bloom summaries for ValueID existence (per valseg + union)
- [ ] T166: Object pull protocol (batch missing ValueIDs, fetch objects, verify hashes)
- [x] T167: Replication metrics: object hit-rate, saved bytes, ref-bytes vs obj-bytes. Latest: added `ReplicationObjectCounters` to the server counters, instrumented `objects.need` / `objects.missing` / `objects.fetch` with hit/miss accounting and byte accounting (hits accumulate `ref_bytes`, fetches accumulate `obj_bytes`), exposed a new `cluster.replication_stats` RPC (read-only, capability-listed) reporting `need_*`, `missing_*`, `fetch_*`, `ref_bytes`, `obj_bytes`, `hit_rate`, `saved_bytes_ratio`, and `last_updated_ms`, and embedded the same JSON under `stats.snapshot.cluster.replication_objects`. One end-to-end integration test (`cluster_replication_stats_tracks_hits_misses_and_bytes`) verifies the counters advance correctly across seed → need → missing → fetch → stats.snapshot.
- [ ] T168: Shard move/rebalance uses object manifests + progress reporting

## Phase 18 - Self-tuning index advisor
- [x] T170: Telemetry feature extraction (predicates/order/group/join keys) + privacy-safe storage
- [x] T171: Candidate index generator + duplication/prefix checks. Latest: advisor synthesis now suppresses exact duplicates, primary-key prefixes, prefixes already covered by existing MySQL-compatible indexes, and any suggestion IDs that were previously applied or dismissed.
- [x] T172: Benefit estimator (Level 0 rule-based) + SkeinQL advisor.* endpoints
- [x] T173: Apply suggestion (CREATE INDEX) + progress + rollback-on-failure. Latest: `advisor.apply_index` now queues background secondary-index builds, `advisor.history` records queued/building/completed/failed lifecycle state with progress percentages, and failed builds record rollback metadata before the suggestion can surface again.
- [x] T174: SkeinAdmin "Index Advisor" page + before/after performance report

## Phase 19 - Time travel and replay bundles
- [x] T180: MVCC as_of reads (planner + executor) + SkeinQL `as_of` parameter (docs/TIME_TRAVEL_REPLAY.md)
- [x] T181: SQL compatibility surface for as_of reads (session variable + query hint). Latest: `MySqlSessionState` now carries a `skein_as_of_ms` field; `SET @@skein.as_of = '<iso>' | <epoch_ms> | NULL | DEFAULT` parses ISO-8601 (with `Z` / `±HH:MM` offsets, fractional seconds) and integer epoch-milliseconds values to control time-travel reads for subsequent SELECTs. Optimizer-style query hint `/*+ SKEIN_AS_OF('<ts>') */` extracted and stripped in `sql_exec` before parsing to override the session value per-statement. Both forms thread through to the MVCC-aware `query_select` as_of filter (T180). 5 unit tests (`parse_as_of_timestamp_accepts_iso_and_epoch_forms`, `parse_skein_as_of_assignment_value_handles_null_default_and_iso`, `extract_skein_as_of_hint_strips_hint_and_returns_epoch_ms`, plus 2 integration tests) cover parsing, session SET/clear, and SELECT filtering via both hint and session variable.
- [x] T182: History retention policy + garbage collection for old versions. Latest: new `maintenance.history.*` RPC surface — `maintenance.history.status` reports per-table live/tombstone/purgeable counts plus `oldest_tombstone_commit_ts_ms`; `maintenance.history.set_policy` persists `history.retention.enabled` and `history.retention.window_ms` via the settings subsystem; `maintenance.history.gc` purges MVCC tombstones whose `commit_ts_ms <= horizon` (explicit params or derived from retention policy). Pre-T180 tombstones (`commit_ts_ms == 0`) are always retained for safety. GC rebuilds the `pk_index`, bumps `table_version` so secondary indexes refresh lazily, clears cached vector indexes, and persists each touched table. `maintenance.history.status` is included in the read-only RPC allowlist alongside `maintenance.compaction.status`. Three engine unit tests (`history_gc_purges_old_tombstones_and_preserves_live_rows`, `history_gc_retains_pre_t180_tombstones`, `history_gc_horizon_filters_recent_tombstones`) cover the basic purge path, the pre-T180 safety retention, and the `commit_ts_ms > horizon` filter.
- [ ] T183: Replay bundle format + export/import tooling + deterministic replay runner
- [ ] T184: SkeinAdmin pages for time travel and replay bundles + integrity status

## Phase 20 - Dedup-preserving encryption
- [ ] T190: Key management + AEAD wrappers (ENC_RANDOM, ENC_MLE_DB) (docs/CONVERGENT_ENCRYPTION.md)
- [ ] T191: ValueStore encryption metadata + encrypt/decrypt paths
- [ ] T192: Key rotation + background re-encryption task + progress reporting
- [ ] T193: settings.encryption.* SkeinQL endpoints + SkeinAdmin UI + audit notes

## Phase 21 - Workload-guided compaction scheduler
- [x] T200: Telemetry signals for compaction (L0 pressure, stalls, latencies) (docs/COMPACTION_SCHEDULER.md). Latest: `stats.snapshot` now scans live `.rseg` segment files for L0 pressure, records bounded soft/hard pressure events, and exposes recent point/range/write rates plus read/write latency percentiles for SkeinAdmin and future scheduler inputs.
- [x] T201: Budget-based compaction scheduler + peak windows + bounds enforcement. Latest: persisted `compaction.*` settings now drive a live heuristic scheduler state in `stats.snapshot.compaction.scheduler`, including configured/effective IO+CPU budgets, peak-window scaling, task priority scoring, and hard-pressure safe-mode write throttling for write-classified SkeinQL/HTTP requests.
- [x] T202: maintenance.compaction.* endpoints (status/set_policy/pause/resume). Latest: `maintenance.compaction.status`, `maintenance.compaction.set_policy`, `maintenance.compaction.pause`, and `maintenance.compaction.resume` now expose and persist runtime scheduler policy through the main RPC surface.
- [x] T203: Evaluation harness scripts + dashboards for stall rate and p99 latency. Latest: `eval/compaction_scheduler_dashboard.py` now emits a deterministic summary JSON, timeline CSV, and self-contained HTML dashboard comparing fixed leveling, fixed tiering, and workload-guided policies on stall rate and p99 latency.

## Phase 22 - SQL autoparameterization and plan cache
- [x] T210: SQL normalization (fingerprints) + parameter extraction (docs/AUTOPARAMETERIZATION.md)
- [x] T211: Plan cache keyed by fingerprint + schema version + session flags
- [x] T212: Integrate autoparam with query coalescing, ETag caching, and telemetry
- [x] T213: SQL session variable: `SET @@skein.autoparameterize = 1` + safety rules
- [x] T214: SkeinAdmin top queries grouped by fingerprint + suggested parameter schemas

## Phase 23 - CDC and dependency-driven changefeeds
- [x] T220: WAL-to-change-event translator (table-level insert/update/delete) (docs/CDC_CHANGEFEED.md). Latest: the persisted CDC change log now records `commit_ts_ms` plus `lsn`-style sequence metadata for table-level insert/update/delete events and acts as the retained WAL-equivalent source for `cdc.poll` / SSE replay.
- [x] T221: cdc.subscribe_table + cdc.poll/cdc.ack/cdc.close endpoints
- [x] T222: Dependency-driven query changefeeds (cdc.subscribe_query) using ETag dependency sets. Latest: prepared queries can now create CDC subscriptions through `cdc.subscribe_query`, and `cdc.poll` emits dependency-driven `invalidate` events carrying the current query ETag whenever a change touches one of the prepared query's dependency tables.
- [x] T223: SSE streaming endpoint + backpressure + reconnect semantics. Latest: `GET /api/v1/cdc/sse/{sub_id}` now streams both table CDC events and query invalidation events as SSE, replays from the in-memory change log in bounded batches, and resumes from `Last-Event-ID` or `from_offset` after reconnects.
- [x] T224: Retention + resnapshot protocol when WAL horizon is exceeded. Latest: bounded retained CDC history now reports `earliest_offset` / `latest_offset`, `cdc.poll` returns explicit `resnapshot_required` responses when a consumer falls behind the retained horizon, and SSE emits a `resnapshot` control event with the same recovery metadata.
- [x] T225: SkeinAdmin CDC page + subscription management + lag visualization. Latest: SkeinAdmin now exposes a dedicated CDC panel for table subscribe/poll/ack/close flows with session-local lag bars and recent-event inspection.

## Phase 24 - Website and documentation site polish
- [x] T230: Homepage: add Docs nav CTA, mobile hamburger menu, maturity badges on feature cards, fix broken links (architecture image, paper), consistent API endpoints
- [x] T231: Docs site homepage: sync with public site (mobile menu, Docs CTA, maturity badges, fixed quickstart endpoint, correct paper link)
- [x] T232: Docs landing (docs.html): add client-side search/filter, mobile menu, polished footer, keyword metadata on cards
- [x] T233: Footer overhaul across all pages — structured 4-column footer with Product/Documentation/Community sections
- [x] T234: Research tracks on public site converted to clickable links pointing to docs/site/research pages

---

## Phase 25 — PostgreSQL wire protocol compatibility
- [x] T400: PG v3 wire protocol primitives (`pg_wire.rs`) — message framing, encode/decode for StartupMessage, RowDescription, DataRow, CommandComplete, ErrorResponse, ParameterStatus, BackendKeyData, Terminate. Includes PG connection handler with simple query protocol, trust/cleartext auth, SSL rejection, and delegation to the shared SQL execution engine. 20 unit tests + 6 integration tests.
- [x] T401: SCRAM-SHA-256 authentication — RFC 5802/7677 SASL exchange with trust mode fallback. Implements full SCRAM-SHA-256 state machine in `pg_wire::scram` module: HMAC-SHA-256, PBKDF2-HMAC-SHA256 (4096 iterations), ScramCredentials (stored_key + server_key derivation), ScramServer (client-first → server-first → client-final → server-final with proof verification). Wire helpers: `write_auth_sasl`, `write_auth_sasl_continue`, `write_auth_sasl_final`, `parse_sasl_initial_response`, `parse_sasl_response`. PG connection handler upgraded from cleartext to SCRAM-SHA-256 when `SKEINDB_TOKEN` is set; trust mode when unset. Deterministic salt derivation via `pg_scram_salt_for_token`. 12 new unit tests (HMAC known vector, PBKDF2, credential derivation, full exchange success/failure, GS2 header rejection, nonce missing, SASL message parsing).
- [x] T402: PG session state — `pg_settings` HashMap on `MySqlSessionState` initialized with 13 PG defaults; `SET key = value` / `SET key TO value` parsing (with LOCAL/SESSION prefix support); `RESET key` / `RESET ALL`; `pg_bootstrap_setting_value` reads session overrides first; `ParameterStatus` sent to client on SET/RESET; `SHOW` and `current_setting()` now reflect session values. 9 unit tests.
- [x] T403: PG connection handler + listener (in `server.rs`) — SSL negotiation (reject with 'N'), startup message parsing, trust/cleartext auth, ParameterStatus batch, BackendKeyData, ReadyForQuery, simple query command loop on port 5432 (configurable via `--pg` flag, default 5432, 0 disables)
- [x] T404: PG SQL dialect parser (`pg_rewrite_sql`) — double-quoted identifiers → backtick-quoted, $$dollar quoting$$ → single-quoted, :: type casts → CAST(… AS …), IS [NOT] DISTINCT FROM → null_safe_eq, FETCH FIRST n ROWS ONLY → LIMIT n, ARRAY[…] → PG array literal string. ILIKE and boolean literals were already implemented. RETURNING deferred to T405 (DML).
- [x] T405: PG DML extensions — `ON CONFLICT DO NOTHING` → `INSERT IGNORE INTO`, `ON CONFLICT (...) DO UPDATE SET ... EXCLUDED.col` → `ON DUPLICATE KEY UPDATE ... VALUES(col)` via `pg_rewrite_on_conflict` post-pass; `INSERT/UPDATE/DELETE ... RETURNING col1, col2, *` extracted and stripped at `pg_dispatch_sql` level with follow-up SELECT using PK lookup for INSERT RETURNING; `COPY FROM STDIN` / `COPY TO STDOUT` returns proper `0A000` (feature not supported) error. 14 unit tests.
- [x] T406: PG DDL — SERIAL/BIGSERIAL/SMALLSERIAL → auto_increment + i64 type, CREATE SCHEMA → CREATE DATABASE alias (with IF NOT EXISTS), CREATE INDEX CONCURRENTLY (accepted/ignored), CREATE INDEX IF NOT EXISTS, COMMENT ON (silently accepted). 9 unit tests.
- [x] T407: PG type OID mapping + encoding — bool→16, i64→20, text→25, jsonb→3802, timestamp→1114, arrays; text + binary format. Added 11 array OID constants (BOOL_ARRAY, INT4_ARRAY, INT8_ARRAY, FLOAT4_ARRAY, FLOAT8_ARRAY, TEXT_ARRAY, VARCHAR_ARRAY, DATE_ARRAY, TIMESTAMP_ARRAY, JSONB_ARRAY, UUID_ARRAY) with `array_element_oid`/`scalar_to_array_oid` utilities. Enhanced type inference heuristic from 3 to 10 types (bool, i64, f64, date, time, datetime, uuid, json, bytes, string). Added `encode_binary_value()` for PG binary wire format (BOOL, INT4, INT8, FLOAT4, FLOAT8, TEXT, VARCHAR, JSON, JSONB, UUID, BYTEA). Bind handler now accepts binary result format codes and stores them in PgPortal; Execute path applies format-aware encoding via `pg_format_code_at` — binary columns use `encode_binary_value`, text columns use `pg_text_value_for_column`. 29 new unit tests.
- [x] T408: PG result encoding — RowDescription, DataRow, CommandComplete ("INSERT 0 1"), ErrorResponse with SQLSTATE codes. Latest: simple and extended PG queries now emit typed text-format `RowDescription` metadata for common numeric/text results, `DataRow` payloads, PG-style `CommandComplete` tags for DML/DDL, and `ErrorResponse` SQLSTATEs end-to-end over the live listener.
- [x] T409: PG system catalogs (`pg_catalog.rs`) — pg_database, pg_namespace, pg_class, pg_attribute, pg_type, pg_index, pg_constraint, pg_proc (stubs), pg_settings, pg_stat_activity. Latest: all ten catalog tables are now served through the shared virtual-table executor, including `pg_class` (tables + index entries with relkind `r`/`i`), `pg_attribute` (column metadata with SkeinDB→PG type OID mapping), `pg_index` (primary key + secondary indexes with `indkey` position vectors), and `pg_constraint` (primary key `p` + unique `u` constraints with `conkey` arrays). Column-level OID overrides ensure correct wire types for all catalog-specific columns (OID, bool, int4, float4).
- [x] T410: PG startup query handling — `SELECT version()`, `current_database()`, `current_schema()`, `SHOW server_version`, `SHOW server_version_num`, `SHOW standard_conforming_strings`, `SHOW max_identifier_length`, `SHOW transaction isolation level`, and `SELECT current_setting(...)` for the common startup/bootstrap probes used by psql/Django/Rails/SQLAlchemy-style clients
- [x] T411: PG extended query protocol — Parse/Bind/Describe/Execute/Sync/Close/Flush, named statements + portals, $1/$2 parameter placeholders. Latest: the PG listener now keeps connection-local prepared statements and portals, supports text-format `Parse` / `Bind` / statement+portal `Describe` / `Execute` / `Close` / `Flush`, substitutes `$1`/`$2` placeholders through the shared SQL engine, and uses `Sync` to recover cleanly after extended-protocol errors.
- [x] T412: PG function mapping (`pg_functions.rs`) — string_agg, array_agg, gen_random_uuid, to_char/to_timestamp, date_trunc, extract(epoch FROM ...), jsonb_build_object, ->>/#>> operators, || concat, ~/~* regex, ARRAY operations, unnest
- [x] T413: PG transaction semantics — ReadyForQuery status byte (I/T/E), failed-tx-block semantics, SAVEPOINT/RELEASE/ROLLBACK TO. Latest: the PG listener now preserves session state across simple queries, emits `ReadyForQuery` as `I` / `T` / `E`, rejects commands in aborted transaction blocks with `25P02`, treats `COMMIT` after an aborted transaction as a rollback, and wires `SAVEPOINT` / `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT` into the existing undo-log bookkeeping.
- [x] T414: PG SQLSTATE error codes — 42P01 (undefined table), 42703 (undefined column), 23505 (unique violation), 42601 (syntax error), etc. Latest: PG simple-query errors now translate shared-engine/MySQL-style failures into PostgreSQL SQLSTATEs, including undefined tables, undefined columns, unique violations, syntax-path parser errors, unsupported features, savepoint lookup failures, and failed-transaction-block errors.
- [x] T415: PG compatibility test corpus (`tests/compat/pg_corpus.sql`) — mirror MySQL corpus structure for PG dialect. Latest: `tests/compat/pg_corpus.sql` now exercises the current PG baseline over the live listener, covering startup probes, shared-engine SQL, and transaction/savepoint behavior through a dedicated `pg_compat_corpus_roundtrip` integration test.
- [x] T416: PG unit tests — SQLSTATE error code mapping (7 tests), type OID mapping for all MySqlStmtColumnType variants, pg_text_value bool normalization + null handling, sql_type_to_desc PG type coverage (serial/boolean/real/decimal/json/blob/timestamp), sql_detect_verb for CREATE/DROP SCHEMA and COMMENT ON, pg_rewrite_sql edge cases (casts in function args, nested parens, mixed features). 19 new tests added in this pass, bringing total PG unit test count to ~40.
- [x] T417: PG integration tests — SCRAM-SHA-256 auth (success + wrong-password rejection), binary result format via extended query, type OID inference for BOOL/INT8/FLOAT8/TEXT literals. 4 new integration tests added covering the T401 SCRAM handshake end-to-end, binary DataRow encoding, and RowDescription type OID correctness.
- [x] T418: PG compatibility documentation (`docs/PG_COMPAT.md`) — refreshed to the current partial baseline (startup/auth, SSL rejection, simple query, tx/savepoint semantics, text-format extended query protocol, and PG corpus coverage) and linked backlog gaps

## Phase 26 - Distribution and installation
- [x] T419: Debian packaging metadata + signed apt repository publication pipeline. Latest: `cargo-deb` metadata is wired into `crates/skeindb/Cargo.toml`, and tagged releases can publish a signed `apt` branch with `Packages`, `Release`, `InRelease`, and exported key material.
- [x] T420: Homebrew tap formula + release automation. Latest: the repo now ships `Formula/skeindb.rb` for tap-based installs, supports immediate `HEAD` installs from this repo, and tagged releases auto-render a stable formula from the release source tarball.

---

## Research Agenda Extensions (Optional)

The repository includes a January 2026 research agenda with 20 proposals.

- Overview: `docs/RESEARCH_AGENDA.md`
- Task-level research tasks (T230+): `docs/RESEARCH_BACKLOG.md`

These items are intentionally separated from the core phases above to keep the main build plan focused.
