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
- [x] T044: Users/privileges + status dashboard. Latest: `admin.user.create` now requires and stores real passwords, DB users persist across restart, MySQL/PG wire auth accepts managed users, and per-database revoke now supports partial privilege removal instead of deleting the whole grant entry.

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
- [ ] T092: audit verify CLI/API + console page

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
- [x] T122: SkeinAdmin security: token UI + role-aware navigation. Latest: dedicated Security panel remains reachable from both sidebar and top-tab navigation; API tokens are now persisted server-side, hashed at rest, shown once on creation, enforced on the HTTP RPC surface, and reflected correctly in the live token/query stats UI.
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
- [ ] T167: Replication metrics: object hit-rate, saved bytes, ref-bytes vs obj-bytes
- [ ] T168: Shard move/rebalance uses object manifests + progress reporting

## Phase 18 - Self-tuning index advisor
- [x] T170: Telemetry feature extraction (predicates/order/group/join keys) + privacy-safe storage
- [ ] T171: Candidate index generator + duplication/prefix checks
- [x] T172: Benefit estimator (Level 0 rule-based) + SkeinQL advisor.* endpoints
- [ ] T173: Apply suggestion (CREATE INDEX) + progress + rollback-on-failure
- [x] T174: SkeinAdmin "Index Advisor" page + before/after performance report

## Phase 19 - Time travel and replay bundles
- [ ] T180: MVCC as_of reads (planner + executor) + SkeinQL `as_of` parameter (docs/TIME_TRAVEL_REPLAY.md)
- [ ] T181: SQL compatibility surface for as_of reads (session variable + query hint)
- [ ] T182: History retention policy + garbage collection for old versions
- [ ] T183: Replay bundle format + export/import tooling + deterministic replay runner
- [ ] T184: SkeinAdmin pages for time travel and replay bundles + integrity status

## Phase 20 - Dedup-preserving encryption
- [ ] T190: Key management + AEAD wrappers (ENC_RANDOM, ENC_MLE_DB) (docs/CONVERGENT_ENCRYPTION.md)
- [ ] T191: ValueStore encryption metadata + encrypt/decrypt paths
- [ ] T192: Key rotation + background re-encryption task + progress reporting
- [ ] T193: settings.encryption.* SkeinQL endpoints + SkeinAdmin UI + audit notes

## Phase 21 - Workload-guided compaction scheduler
- [ ] T200: Telemetry signals for compaction (L0 pressure, stalls, latencies) (docs/COMPACTION_SCHEDULER.md)
- [ ] T201: Budget-based compaction scheduler + peak windows + bounds enforcement
- [ ] T202: maintenance.compaction.* endpoints (status/set_policy/pause/resume)
- [ ] T203: Evaluation harness scripts + dashboards for stall rate and p99 latency

## Phase 22 - SQL autoparameterization and plan cache
- [x] T210: SQL normalization (fingerprints) + parameter extraction (docs/AUTOPARAMETERIZATION.md)
- [x] T211: Plan cache keyed by fingerprint + schema version + session flags
- [x] T212: Integrate autoparam with query coalescing, ETag caching, and telemetry
- [x] T213: SQL session variable: `SET @@skein.autoparameterize = 1` + safety rules
- [x] T214: SkeinAdmin top queries grouped by fingerprint + suggested parameter schemas

## Phase 23 - CDC and dependency-driven changefeeds
- [ ] T220: WAL-to-change-event translator (table-level insert/update/delete) (docs/CDC_CHANGEFEED.md)
- [x] T221: cdc.subscribe_table + cdc.poll/cdc.ack/cdc.close endpoints
- [ ] T222: Dependency-driven query changefeeds (cdc.subscribe_query) using ETag dependency sets
- [ ] T223: SSE/WebSocket streaming endpoint + backpressure + reconnect semantics
- [ ] T224: Retention + resnapshot protocol when WAL horizon is exceeded
- [ ] T225: SkeinAdmin CDC page + subscription management + lag visualization

## Phase 24 - Website and documentation site polish
- [x] T230: Homepage: add Docs nav CTA, mobile hamburger menu, maturity badges on feature cards, fix broken links (architecture image, paper), consistent API endpoints
- [x] T231: Docs site homepage: sync with public site (mobile menu, Docs CTA, maturity badges, fixed quickstart endpoint, correct paper link)
- [x] T232: Docs landing (docs.html): add client-side search/filter, mobile menu, polished footer, keyword metadata on cards
- [x] T233: Footer overhaul across all pages — structured 4-column footer with Product/Documentation/Community sections
- [x] T234: Research tracks on public site converted to clickable links pointing to docs/site/research pages

---

## Phase 25 — PostgreSQL wire protocol compatibility
- [x] T400: PG v3 wire protocol primitives (`pg_wire.rs`) — message framing, encode/decode for StartupMessage, RowDescription, DataRow, CommandComplete, ErrorResponse, ParameterStatus, BackendKeyData, Terminate. Includes PG connection handler with simple query protocol, trust/cleartext auth, SSL rejection, and delegation to the shared SQL execution engine. 20 unit tests + 6 integration tests.
- [ ] T401: SCRAM-SHA-256 authentication (`pg_auth.rs`) — RFC 5802/7677 exchange + trust mode
- [ ] T402: PG session state (`pg_session.rs`) — search_path, DateStyle, TimeZone, tx state (I/T/E), client_encoding, standard_conforming_strings
- [x] T403: PG connection handler + listener (in `server.rs`) — SSL negotiation (reject with 'N'), startup message parsing, trust/cleartext auth, ParameterStatus batch, BackendKeyData, ReadyForQuery, simple query command loop on port 5432 (configurable via `--pg` flag, default 5432, 0 disables). Latest: cleartext auth now accepts managed SkeinDB DB users in addition to the legacy `SKEINDB_TOKEN` override.
- [ ] T404: PG SQL dialect parser (`pg_parse.rs`) — double-quoted identifiers, $$dollar quoting$$, :: type casts, RETURNING, ILIKE, IS DISTINCT FROM, FETCH FIRST n ROWS ONLY, ARRAY[...], boolean literals
- [ ] T405: PG DML extensions — INSERT/UPDATE/DELETE...RETURNING, ON CONFLICT DO NOTHING/UPDATE, basic COPY FROM STDIN / TO STDOUT
- [ ] T406: PG DDL — SERIAL/BIGSERIAL → auto_increment, CREATE SCHEMA → database, CREATE INDEX CONCURRENTLY (accept/ignore), COMMENT ON
- [ ] T407: PG type OID mapping + encoding (`pg_types.rs`) — bool→16, i64→20, text→25, jsonb→3802, timestamp→1114, arrays; text + binary format
- [ ] T408: PG result encoding — RowDescription, DataRow, CommandComplete ("INSERT 0 1"), ErrorResponse with SQLSTATE codes
- [ ] T409: PG system catalogs (`pg_catalog.rs`) — pg_database, pg_namespace, pg_class, pg_attribute, pg_type, pg_index, pg_constraint, pg_proc (stubs), pg_settings, pg_stat_activity
- [x] T410: PG startup query handling — `SELECT version()`, `current_database()`, `current_schema()`, `SHOW server_version`, `SHOW server_version_num`, `SHOW standard_conforming_strings`, `SHOW max_identifier_length`, `SHOW transaction isolation level`, and `SELECT current_setting(...)` for the common startup/bootstrap probes used by psql/Django/Rails/SQLAlchemy-style clients
- [ ] T411: PG extended query protocol — Parse/Bind/Describe/Execute/Sync/Close/Flush, named statements + portals, $1/$2 parameter placeholders
- [ ] T412: PG function mapping (`pg_functions.rs`) — string_agg, array_agg, gen_random_uuid, to_char/to_timestamp, date_trunc, extract(epoch FROM ...), jsonb_build_object, ->>/#>> operators, || concat, ~/~* regex, ARRAY operations, unnest
- [ ] T413: PG transaction semantics — ReadyForQuery status byte (I/T/E), failed-tx-block semantics, SAVEPOINT/RELEASE/ROLLBACK TO. Latest: the simple-query path now enters `ReadyForQuery(E)` after statement errors inside explicit transactions and rejects subsequent commands until `ROLLBACK`/failed-`COMMIT` cleanup, but SAVEPOINT semantics and broader transaction/session parity remain open.
- [ ] T414: PG SQLSTATE error codes — 42P01 (undefined table), 42703 (undefined column), 23505 (unique violation), 42601 (syntax error), etc.
- [ ] T415: PG compatibility test corpus (`tests/compat/pg_corpus.sql`) — mirror MySQL corpus structure for PG dialect
- [ ] T416: PG unit tests — wire round-trips, SCRAM vectors, SQL parse, type encode/decode, catalog queries
- [ ] T417: PG integration tests — psql end-to-end, psycopg2, node-postgres driver tests
- [x] T418: PG compatibility documentation (`docs/PG_COMPAT.md`) — refreshed to the current partial baseline (startup/auth, SSL rejection, simple query, tx stubs, extended-protocol stubs) and linked backlog gaps

---

## Research Agenda Extensions (Optional)

The repository includes a January 2026 research agenda with 20 proposals.

- Overview: `docs/RESEARCH_AGENDA.md`
- Task-level research tasks (T230+): `docs/RESEARCH_BACKLOG.md`

These items are intentionally separated from the core phases above to keep the main build plan focused.
