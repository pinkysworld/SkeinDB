# SkeinDB Project Backlog

This backlog is designed for small PR-sized tasks.
Each task should include tests.

## Reality sync (2026-03-06)

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
- Status: baseline protocol + corpus-oriented SQL compatibility are implemented in runtime/tests; remaining items below are parity and driver-hardening follow-ups.
- [x] T030: Handshake + mysql_native_password
- [x] T031: COM_QUERY SELECT literals
- [x] T032: SQL translator (subset)
- [x] T033: DDL/DML subset for corpus.sql
- [x] T034: SQL_CALC_FOUND_ROWS + FOUND_ROWS
- [ ] T035: Index-backed secondary/unique index enforcement for MySQL duplicate-key semantics (runtime write-path now uses in-memory unique probe indexes, creating MySQL compatibility `UNIQUE INDEX` definitions now rejects pre-existing duplicate rows, and MySQL compatibility `KEY` / `UNIQUE KEY` metadata now seeds the prototype's in-memory secondary-index prefilter path; durable/reusable secondary-index lifecycle and full parity hardening still remain)
- [ ] T036: Broaden COM_QUERY parity for WordPress-class workloads (single-column grouped `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` shims, projection-grouped `GROUP BY` de-dup compatibility for `SQL_CALC_FOUND_ROWS` flows, parenthesized `AND`/`OR` filters plus `NOT IN` / `NOT LIKE`, basic left-associative multi-join chains, `CREATE [UNIQUE] INDEX` / `DROP INDEX`, `ALTER TABLE ... ADD COLUMN` position-clause / `MODIFY [COLUMN]` / `CHANGE [COLUMN]` / `RENAME COLUMN` / `DROP COLUMN` / `ADD [UNIQUE] KEY` / `DROP [KEY|INDEX]`, broader scalar-function translation for `LOWER` / `UPPER` / `LENGTH` / `CHAR_LENGTH` / `TRIM` / `LTRIM` / `RTRIM` / `LEFT` / `RIGHT` / `SUBSTRING` / `SUBSTR` / `REPLACE` / `NULLIF` / `IF` / `LOCATE` / `INSTR` / `ABS` / `ROUND` / `FLOOR` / `CEIL` / `CEILING` / `MOD` / `LEAST` / `GREATEST` / `COALESCE` / `IFNULL` / `CONCAT`, bootstrap/session compatibility `SET` forms including `SQL_AUTO_IS_NULL`, literal `SELECT @@...` compatibility for `LIMIT` / `OFFSET` bootstrap probes, compatibility `SHOW VARIABLES` / `SHOW STATUS` / `SHOW CHARACTER SET` / `SHOW COLLATION` values (including unfiltered, scoped, simple `WHERE ...` filters, and wildcard forms such as `character_set_%` / `collation_%`), top-level `AND`-chain `IN (SELECT ...)` / `[NOT] EXISTS (SELECT ...)` rewrites plus equality-based correlated `IN` / multi-column `EXISTS` membership rewrites, and `LOCK TABLES` / `UNLOCK TABLES` no-op handling are now covered; deeper correlated/nested subqueries, broader scalar/function parity, and broader `ALTER TABLE` variants still remain)
- [ ] T037: Deepen COM_STMT parity beyond the current baseline (complex-query result metadata, stricter driver/cursor semantics, fuller protocol coverage; prepare-time metadata now also covers supported scalar-expression projections plus simple aggregate / grouped-aggregate compatibility queries)

## Phase 4 — Web console
- [x] T040: HTTP API `/api/v1/sql/exec`
- [x] T041: Console UI scaffold
- [x] T042: Schema browser + SQL editor
- [ ] T043: Data browse/edit + import/export
- [ ] T044: Users/privileges + status dashboard

## Phase 5 - SkeinQL native API
- [x] T050: Define SkeinQL request/response types + error model (docs/SKEINQL.md)
- [x] T051: Implement HTTP RPC endpoint POST /api/v1/rpc (system.ping, system.version)
- [x] T052: Implement schema.* methods (list/describe/create/drop)
- [x] T053: Implement query.select (single-table scan + filter + limit) over SkeinIR
- [x] T054: Implement tx.begin/commit/rollback via SkeinQL

## Phase 6 - Cache-coherent HTTP queries (ETags)
- [x] T060: Row ETags for data.get and If-Match support for data.update
- [x] T061: Planner dependency sets for simple indexed queries
- [ ] T062: query.prepare + GET /api/v1/q/{query_id} with ETag/If-None-Match
- [ ] T063: SSE subscription to ETag changes (query.subscribe)

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
- [ ] T090: WALHeader v2 with hash chaining (docs/AUDIT_WAL.md)
- [ ] T091: checkpoint anchors + audit status
- [ ] T092: audit verify CLI/API + console page

## Phase 10 - Hybrid row/column snapshots
- [ ] T100: Snapshot builder (scan MVCC at snapshot_ts) + cseg writer (docs/COLUMN_SNAPSHOTS.md)
- [ ] T101: Snapshot reader + column scan operator
- [ ] T102: Optimizer rule: use column snapshots for covered ranges

## Phase 11 - Compatibility telemetry and migration hints
- [ ] T110: Feature flag instrumentation in MySQL translator
- [ ] T111: Internal storage for telemetry counters + query fingerprints (optional)
- [ ] T112: telemetry.compat_summary endpoint + console dashboard
- [ ] T113: telemetry.migration_hints generator (MySQL patterns -> SkeinQL calls)

## Phase 12 - Standalone management console (SkeinAdmin)
- [x] T120: SkeinAdmin placeholder scaffold (web/skeinadmin) + connection profiles
- [x] T121: SkeinAdmin pages: schema/data/sql workspace
- [ ] T122: SkeinAdmin security: token UI + role-aware navigation
- [x] T123: SkeinAdmin cluster page (cluster.*) + actions
- [x] T124: SkeinAdmin observability page (stats.*) — comprehensive dashboard with runtime, storage/dedup, MVCC/compaction, query/cache stats + auto-refresh
- [x] T125: SkeinAdmin Easy Viewer (phpMyAdmin-inspired) — sidebar tree, sub-tabs, inline editing, search, export, operations
- [x] T126: SkeinAdmin Engine Config panel — checkbox toggles for dedup, compression, encryption, MVCC, delta chains, time travel, compaction, cache, security, replication, CDC, QUIC

## Phase 13 - Observability and server load statistics
- [x] T130: stats.snapshot and basic counters in server
- [x] T131: query fingerprinting + top_queries / slow_queries
- [x] T132: GET /metrics (Prometheus-style) + labels
- [x] T133: Console widgets for CPU/memory/disk/QPS/TPS/compaction — Overview dashboard with stat cards, dedup bar chart, auto-refresh

## Phase 14 - Cluster management and scale-out
- [x] T140: Node identity (node_id) + cluster config model
- [x] T141: Replication transport protocol (primary -> replica fanout over SkeinQL RPC)
- [ ] T142: CAS object pull protocol (replica fetch missing ValueIDs)
- [ ] T143: Read-only replica serving + router (read balancing)
- [x] T144: cluster.* SkeinQL endpoints + join tokens + promote replica
- [x] T145: Sharding metadata + router prototype (single-shard txns)

## Phase 15 - Additional performance improvements
- [ ] T150: Schema flag for interned columns + ValueID-first predicate ops (docs/PERFORMANCE.md)
- [ ] T151: Late materialization (decode only projected columns)
- [ ] T152: Batch (vectorized) scan/filter/project pipeline
- [ ] T153: MVCC Visible Version Index cache

## Phase 16 - Query coalescing (thundering herd protection)
- [ ] T160: Query fingerprint canonicalization (SkeinIR + SkeinQL) + auth scope keying
- [ ] T161: In-flight query map (leader/joiner) with cancellation semantics
- [ ] T162: Enable coalescing for GET /api/v1/q/{query_id} (cacheable) + tests
- [ ] T163: Metrics + limits + SkeinAdmin dashboard widget

## Phase 17 - CAS-aware replication bandwidth bounds (object-aware sync)
- [ ] T165: Bloom summaries for ValueID existence (per valseg + union)
- [ ] T166: Object pull protocol (batch missing ValueIDs, fetch objects, verify hashes)
- [ ] T167: Replication metrics: object hit-rate, saved bytes, ref-bytes vs obj-bytes
- [ ] T168: Shard move/rebalance uses object manifests + progress reporting

## Phase 18 - Self-tuning index advisor
- [ ] T170: Telemetry feature extraction (predicates/order/group/join keys) + privacy-safe storage
- [ ] T171: Candidate index generator + duplication/prefix checks
- [x] T172: Benefit estimator (Level 0 rule-based) + SkeinQL advisor.* endpoints
- [ ] T173: Apply suggestion (CREATE INDEX) + progress + rollback-on-failure
- [ ] T174: SkeinAdmin "Index Advisor" page + before/after performance report

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
- [ ] T211: Plan cache keyed by fingerprint + schema version + session flags
- [ ] T212: Integrate autoparam with query coalescing, ETag caching, and telemetry
- [ ] T213: SQL session variable: `SET @@skein.autoparameterize = 1` + safety rules
- [ ] T214: SkeinAdmin top queries grouped by fingerprint + suggested parameter schemas

## Phase 23 - CDC and dependency-driven changefeeds
- [ ] T220: WAL-to-change-event translator (table-level insert/update/delete) (docs/CDC_CHANGEFEED.md)
- [ ] T221: cdc.subscribe_table + cdc.poll/cdc.ack endpoints
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

## Research Agenda Extensions (Optional)

The repository includes a January 2026 research agenda with 20 proposals.

- Overview: `docs/RESEARCH_AGENDA.md`
- Task-level research tasks (T230+): `docs/RESEARCH_BACKLOG.md`

These items are intentionally separated from the core phases above to keep the main build plan focused.
