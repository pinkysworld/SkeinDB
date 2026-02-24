# SkeinDB Project Backlog (Codex-friendly)

This backlog is designed for small PR-sized tasks.
Each task should include tests.

## Reality sync (2026-02-24)

This file now reflects a stricter distinction:
- `[x]` = implemented and exercised in runtime/tests.
- `[ ]` = still open (including prototype/partial work that still needs hardening).

For the full implemented-vs-partial matrix (core + research), see:
- `docs/TRUE_STATUS_MATRIX.md`

## Phase 0 — Repo setup
- [x] T001: Encoding primitives (VarU, Bytes/String, CRC32C)
- [ ] T002: FileHeader read/write
- [ ] T003: RecordFrame append/iterate

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
- [ ] T021: information_schema.tables + columns
- [x] T022: Minimal executor: CREATE TABLE, INSERT, SELECT scan+filter+limit

## Phase 3 — MySQL protocol
- [ ] T030: Handshake + mysql_native_password
- [ ] T031: COM_QUERY SELECT literals
- [ ] T032: SQL translator (subset)
- [ ] T033: DDL/DML subset for corpus.sql
- [ ] T034: SQL_CALC_FOUND_ROWS + FOUND_ROWS

## Phase 4 — Web console
- [ ] T040: HTTP API `/api/v1/sql/exec`
- [x] T041: Console UI scaffold
- [x] T042: Schema browser + SQL editor
- [ ] T043: Data browse/edit + import/export
- [ ] T044: Users/privileges + status dashboard

## Phase 5 - SkeinQL native API
- [x] T050: Define SkeinQL request/response types + error model (docs/SKEINQL.md)
- [x] T051: Implement HTTP RPC endpoint POST /api/v1/rpc (system.ping, system.version)
- [ ] T052: Implement schema.* methods (list/describe/create/drop)
- [x] T053: Implement query.select (single-table scan + filter + limit) over SkeinIR
- [ ] T054: Implement tx.begin/commit/rollback via SkeinQL

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
- [ ] T124: SkeinAdmin observability page (stats.*)

## Phase 13 - Observability and server load statistics
- [x] T130: stats.snapshot and basic counters in server
- [ ] T131: query fingerprinting + top_queries / slow_queries
- [x] T132: GET /metrics (Prometheus-style) + labels
- [ ] T133: Console widgets for CPU/memory/disk/QPS/TPS/compaction

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

---

## Research Agenda Extensions (Optional)

The repository includes a January 2026 research agenda with 20 proposals.

- Overview: `docs/RESEARCH_AGENDA.md`
- Codex-friendly research tasks (T230+): `docs/RESEARCH_BACKLOG.md`

These items are intentionally separated from the core phases above to keep the main build plan focused.
