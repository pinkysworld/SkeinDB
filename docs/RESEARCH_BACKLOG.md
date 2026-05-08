# Research Backlog (Adapted from January 2026 agenda)

This backlog turns the **20 research proposals** in `docs/RESEARCH_AGENDA.md` into **Codex-friendly, PR-sized tasks**, mapped onto the existing Phase A–G build plan and the current `docs/PROJECT_BACKLOG.md` task numbering.

Notes:
- These items are **research-oriented**: the goal is to make each direction implementable and measurable.
- Tasks are designed to be **optional** and do not block core MySQL compatibility.

## Reality sync (2026-05-08)

Runtime status and checklist status intentionally differ:
- Runtime: all R01-R20 tracks have prototype coverage in code/method surfaces/tests.
- **18 tracks are now hardened** with real algorithms and dedicated tests:
  - **R01** — Learned indexes (hybrid learned+fallback read path, refresh policy, benchmark and distribution-shift coverage)
  - **R02** — Adaptive row/column storage (snapshot + readback integration test)
  - **R03** — Delta topology analysis (hot-chain detection, topology reports)
  - **R04** — Differential privacy (Laplace noise, RDP composition, budget tracking)
  - **R05** — Oblivious execution (policy registration + padding verification test)
  - **R06** — Forensic Merkle proofs (SHA-256 hash chains, inclusion verification)
  - **R07** — Client-side merge functions (conflict resolution + merge.list test)
  - **R08** — Incremental view maintenance (dependency graphs, cascading invalidation)
  - **R09** — QUIC-native protocol (multi-stream RPCs + rebind verification test)
  - **R10** — HNSW vector search (M=16, ef=200, cosine similarity, multi-layer graph)
  - **R11** — LLM-assisted autoparameterization (classify + analyze integration test)
  - **R12** — Natural language to SkeinQL (prompt packaging, dependency explanation, preview rows, approval-gated execution, evaluation harness)
  - **R13** — Causal vector-clock ETags (V2 clocks, dependency tracking, stale detection)
  - **R14** — Geo-distributed replay bundles (bundle request/apply/status roundtrip test)
  - **R15** — Conflict-free schema evolution (propose/merge/apply integration test)
  - **R16** — Auto index synthesis (workload-driven advisor.recommend integration test)
  - **R17** — Query intent migration (pattern library, sequence intent detection, rewrite previews, JSON/Markdown report export)
  - **R20** — Energy-aware compaction (energy model, constrained scheduler, external signals, energy-vs-p99 harness)
- Checklist below: remains open for further hardening, stronger benchmarks, and publication-grade evaluation.
- This sync promotes R12 to hardened (in addition to the previous batches); 2 tracks remain at prototype level.
- 2026-04-26: T230 is closed with exportable `ValueStore::lookup_distribution()` histograms and `stats.snapshot.storage.value_lookup` evidence.
- 2026-04-26: T231 is closed with `ValueStore::learned_index_report()` exposing offline-built segment metadata and fallback index sizing.
- 2026-05-08: T232-T235 are closed with the feature-flagged hybrid learned lookup path, compaction/insert-triggered refresh policy, `ValueStore::benchmark()` probe quantiles, and distribution-shift fallback tests.
- 2026-05-08: T073-T076 are closed with periodic delta snapshots, geometric skip patches, compaction-time delta rewrites / skip rebuilds, `topology_analysis()`, `delta_benchmark()`, and focused ValueStore tests.
- 2026-05-08: T204-T207 are closed with `energy_aware` compaction policy support, CPU/IO energy estimates, persisted external power/price/carbon signals, SkeinAdmin controls, and deterministic energy-vs-p99 evaluation output.
- 2026-05-08: T114-T118 are closed with intent-pattern detection for pagination/polling/soft deletes/hierarchies/EXISTS/defaults, sequence-level polling correlation, SkeinQL-native rewrite snippets, SkeinAdmin migration assistant wiring, and the `migration.report_export` JSON/Markdown exporter.
- 2026-05-08: T310-T316 are closed with `ai.nl.translate` prompt packages and rule translation, dependency-backed `ai.nl.explain` summaries plus preview rows, approval-token-gated `ai.nl.execute`, the `skeindb nl-eval` execution-match harness, SkeinAdmin NL Lab wiring, and focused engine/RPC/eval tests.

Source of truth matrix:
- `docs/TRUE_STATUS_MATRIX.md`

## Mapping table (proposal → repo)

| ID | Proposal | Priority (agenda) | Primary repo specs / files | Backlog tasks |
|---:|---|---|---|---|
| 1 | Learned Index Structures for ValueID Lookup | P2 | `docs/research_agenda/R01_*` | **Phase 24** (T230–T235) |
| 2 | Adaptive Row-Column Hybrid Execution | — | `docs/COLUMN_SNAPSHOTS.md` + `docs/research_agenda/R02_*` | Extend **Phase 10** (T103–T106) |
| 3 | Delta-Chain Topology Optimization | P1 | `docs/DELTA_VALUES.md` + `docs/research_agenda/R03_*` | Extend **Phase 7** (T073–T076) |
| 4 | Differentially Private Aggregate Queries | P0 | `docs/research_agenda/R04_*` | **Phase 25** (T240–T246) |
| 5 | Oblivious Query Execution | — | `docs/research_agenda/R05_*` | **Phase 26** (T250–T256) |
| 6 | Forensic Query Language for Hash-Chained WAL | P1 | `docs/AUDIT_WAL.md` + `docs/research_agenda/R06_*` | **Phase 27** (T260–T266) |
| 7 | Optimistic Concurrency with Client-Side Merge Functions | P1 | `docs/WASM_UDFS.md` + `docs/ETAG_VALIDATORS.md` + `docs/research_agenda/R07_*` | **Phase 28** (T270–T276) |
| 8 | Incremental View Maintenance via Dependency Graphs | P0 | `docs/ETAG_VALIDATORS.md` + `docs/CDC_CHANGEFEED.md` + `docs/research_agenda/R08_*` | **Phase 29** (T280–T287) |
| 9 | HTTP/3 and QUIC-Native DB Protocol | P2 | `docs/research_agenda/R09_*` | **Phase 30** (T290–T295) |
| 10 | Vector Embeddings as First-Class ValueIDs | P0 | `docs/research_agenda/R10_*` | **Phase 31** (T300–T307) |
| 11 | LLM-Assisted Query Autoparameterization | — | `docs/AUTOPARAMETERIZATION.md` + `docs/research_agenda/R11_*` | Extend **Phase 22** (T215–T218) |
| 12 | Natural Language to SkeinQL with Verification | — | `docs/research_agenda/R12_*` | **Phase 32** (T310–T316) |
| 13 | Causal Consistency via ETag Chains | P0 | `docs/ETAG_VALIDATORS.md` + `docs/research_agenda/R13_*` | Extend **Phase 6** (T064–T067) |
| 14 | Geo-Distributed Replay Bundles for Edge Caching | P2 | `docs/TIME_TRAVEL_REPLAY.md` + `docs/research_agenda/R14_*` | Extend **Phase 19** (T185–T188) |
| 15 | Conflict-Free Schema Evolution | — | `docs/research_agenda/R15_*` | **Phase 33** (T320–T326) |
| 16 | Automatic Index Synthesis from Dependency Analysis | P1 | `docs/INDEX_ADVISOR.md` + `docs/research_agenda/R16_*` | Extend **Phase 18** (T175–T179) |
| 17 | Query Intent Inference for Compatibility Migration | — | `docs/TELEMETRY_AND_MIGRATION.md` + `docs/research_agenda/R17_*` | Extend **Phase 11** (T114–T118) |
| 18 | Reproducible Performance Regression Testing | — | `docs/TIME_TRAVEL_REPLAY.md` + `docs/research_agenda/R18_*` | Extend **Phase 19** (T189) |
| 19 | WebAssembly-Native Query Operators | P2 | `docs/WASM_UDFS.md` + `docs/research_agenda/R19_*` | Extend **Phase 8** (T084–T087) |
| 20 | Energy-Aware Compaction Scheduling | — | `docs/COMPACTION_SCHEDULER.md` + `docs/research_agenda/R20_*` | Extend **Phase 21** (T204–T207) |

## Task definitions (new additions)

### Phase 24 — Learned indexes for ValueID lookup (R01)
- [x] T230: Instrument ValueID lookup distribution + export histograms
- [x] T231: Prototype learned model index (offline build) with fallback structure
- [x] T232: Integrate hybrid learned+fallback lookup into ValueStore read path (feature flag). Evidence: `ValueStoreConfig.enable_learned_index`, `ValueStore::get_with_trace`, and `learned_index_lookup_hits` / `learned_index_falls_back_for_new_keys`.
- [x] T233: Compaction-time model refresh policy + correctness tests. Evidence: `ModelRefreshPolicy`, `ValueStore::should_refresh`, `maybe_refresh`, `refresh_learned_index`, insert-triggered refresh checks, and `distribution_shift_triggers_refresh`.
- [x] T234: Benchmark harness: lookup p50/p99/p99.9 + memory overhead. Evidence: `ValueStore::benchmark()` and `benchmark_reports_quantiles`.
- [x] T235: Distribution shift tests + graceful degradation. Evidence: `ValueIdLookupDistribution::model_shift_l1`, `learned_index_falls_back_for_new_keys`, and `distribution_shift_triggers_refresh`.

### Phase 25 — Differential privacy aggregates (R04)
- [ ] T240: Add SkeinQL aggregate nodes (COUNT/SUM/AVG) with explicit DP parameters (experimental)
- [ ] T241: Sensitivity analysis for single-table aggregates (bounded domains)
- [ ] T242: Privacy budget manager (per user/role) + persistence
- [ ] T243: Noise mechanisms (Laplace / Gaussian policy) + deterministic tests (seeded RNG)
- [ ] T244: Privacy-aware caching rules (ETag includes privacy metadata)
- [ ] T245: Audit log entries for DP queries (budget consumption)
- [ ] T246: Evaluation harness: accuracy vs epsilon, overhead vs baseline

### Phase 26 — Oblivious query execution (R05)
- [ ] T250: Threat model doc + “obliviousness levels” policy schema
- [ ] T251: ValueStore lookup padding + dummy reads (table/column policy)
- [ ] T252: Oblivious scan primitive (fixed-size batches, padding)
- [ ] T253: Oblivious sort/join primitive (limited scope, research mode)
- [ ] T254: Leakage evaluation harness (trace-based, mutual information metrics)
- [ ] T255: Performance overhead report generator
- [ ] T256: Admin UI settings for per-table obliviousness levels

### Phase 27 — Forensic query language (R06)
- [ ] T260: Define SkeinForensic query grammar (minimal) + JSON form over SkeinQL
- [ ] T261: Build verifiable WAL index (time/table/user) consistent with hash chain
- [ ] T262: Proof format for inclusion + boundary proofs; verifier tool
- [ ] T263: `forensic.query` SkeinQL endpoint + exportable report bundles
- [ ] T264: Incremental verification via checkpoint anchors
- [ ] T265: Case-study harness: simulated incident timelines + proofs
- [ ] T266: SkeinAdmin “Forensics” page (query + verify + export)

### Phase 28 — Merge functions for optimistic concurrency (R07)
- [ ] T270: Conflict model (write-write, constraint, dependency) + detection hooks
- [ ] T271: Merge function registry (Wasm) + capability model (“values-only” access)
- [ ] T272: SkeinQL `merge.register` / `merge.apply` + SQL compat hook (If-Match)
- [ ] T273: Offline write queue format (client SDK spec) + merge result handling
- [ ] T274: Safety tests: cancellation + deterministic merges
- [ ] T275: Bench: conflict rate + resolution success on example workloads
- [ ] T276: SkeinAdmin “Merge rules” page

### Phase 29 — Incremental view maintenance (R08)
- [ ] T280: `view.create` SkeinQL method with persisted definition (SkeinIR)
- [ ] T281: Dependency graph extension: view → base table deps at column granularity
- [ ] T282: Delta derivation for a restricted operator set (filter, project, group-by)
- [ ] T283: Incremental refresh pipeline (apply deltas from CDC stream)
- [ ] T284: Cost-based switch: incremental vs full recompute
- [ ] T285: Correctness oracle: compare incremental vs recompute on random workloads
- [ ] T286: Bench: view maintenance overhead + query speedups
- [ ] T287: SkeinAdmin “Views” page (status, refresh, explain deps)

### Phase 30 — HTTP/3 / QUIC-native protocol (R09)
- [ ] T290: Protocol sketch: SkeinQL-over-QUIC framing + stream mapping
- [ ] T291: Implement server prototype with a QUIC library (feature-flag)
- [ ] T292: Prepared query handles over QUIC streams (read-only first)
- [ ] T293: 0-RTT safety rules (no writes in 0-RTT by default)
- [ ] T294: Bench: p99 latency under concurrency vs HTTP/2 and MySQL/TCP
- [ ] T295: Connection migration test harness (simulated IP change)

### Phase 31 — Vector embeddings as first-class ValueIDs (R10)
- [ ] T300: Add `ValueKind::Embedding` and typed literal support in SkeinQL
- [ ] T301: LSH bucket + content hash ValueID scheme (exact + approximate id)
- [ ] T302: ANN search operator (bucket filter + distance refine) (baseline)
- [ ] T303: Hybrid query: filter predicates + ANN order-by
- [ ] T304: Dependency tracking for embedding-derived queries (invalidate on source change)
- [ ] T305: Bench harness: recall/latency vs baseline index
- [ ] T306: Example app: small RAG retrieval pipeline
- [ ] T307: SkeinAdmin “Embeddings” page (index status + query playground)

### Phase 32 — Natural language to SkeinQL with verification (R12)
- [x] T310: NL→SkeinQL prompt+schema packaging format (offline first). Evidence: `AiNlPromptPackage`, `build_nl_prompt`, `ai.nl.translate`, and `ai_nl_translate_packages_schema` / `ai_nl_translate_explain_execute_roundtrip`.
- [x] T311: Query explanation generator from dependency sets + planner info. Evidence: `ai.nl.explain`, `dependencies_for_query`, `ai_nl_preview`, and explanation fields for tables/projection/filters/order/limit/deps.
- [x] T312: Verification UI flow: explanation + sample rows + approval gate. Evidence: SkeinAdmin NL Lab query JSON / preview / approval-token controls plus `ai_nl_translate_explain_execute_rpc_roundtrip`.
- [x] T313: Safety policy: forbid writes unless explicit confirmation token. Evidence: the SkeinQL `Query` shape is read-query-only for this surface, `ai.nl.explain` only accepts SELECT, `ai.nl.execute` recomputes the approval token from query+args+deps, and tampered-query execution is rejected in `ai_nl_translate_explain_execute_roundtrip`.
- [x] T314: Evaluation harness: adapted text-to-SQL benchmarks (execution match). Evidence: `skeindb nl-eval`, `NlEvalReport.execution_matches`, `eval_examples_exact_and_exec_match`, and `eval_examples_uses_rule_translation_for_execution_match`.
- [x] T315: Iterative refinement protocol (user feedback loop). Evidence: prompt packages include stable `fingerprint`, editable generated query JSON, explicit args, preview re-explain, and reapproval before execution in SkeinAdmin.
- [x] T316: SkeinAdmin “NL Query” page (experimental). Evidence: NL Lab (R11-R12) panel wires `ai.nl.translate`, `ai.nl.explain`, and `ai.nl.execute` with approval-token-gated execution.

### Phase 33 — Conflict-free schema evolution (R15)
- [ ] T320: Schema version tagging in MVCC row versions
- [ ] T321: Concurrent schema changes protocol (add column/index) + conflict detection
- [ ] T322: Query execution across schema heterogeneity (safe conversions)
- [ ] T323: Schema merge algorithm + roll-forward/rollback rules
- [ ] T324: Migration assistant: show divergence + propose resolution
- [ ] T325: Rolling-deploy simulation harness
- [ ] T326: SkeinAdmin “Schema evolution” page

## Extensions to existing phases (inline patches)

The following additions extend existing phases in `docs/PROJECT_BACKLOG.md`.

### Extend Phase 6 — Causal ETag chains (R13)
- [ ] T064: Define causal ETag format (compressed dependencies / vector-clock hybrid)
- [ ] T065: `min_causality` request field + response causality propagation rules
- [ ] T066: Replication propagates causality metadata (no total order required)
- [ ] T067: Cache interaction tests (If-None-Match with causal validators)

### Extend Phase 7 — Delta topology optimization (R03)
- [x] T073: Implement periodic full snapshots for deltas (K-depth policy). Evidence: `DeltaPolicy.snapshot_interval`, `ValueStore::put_with_delta`, and `delta_snapshot_interval_enforces_raw`.
- [x] T074: Skip-pointer (skip-list) delta chain encoding. Evidence: `SkipPatch`, geometric `build_skip_patches`, `materialize_with_trace`, and `skip_patches_reduce_steps`.
- [x] T075: Compaction-time topology restructuring policy. Evidence: `ValueStore::compact_deltas`, `DeltaCompactionReport`, and `delta_compaction_rewrites_deep_chains`.
- [x] T076: Bench: reconstruction latency vs write amplification. Evidence: `ValueStore::delta_benchmark()` p50/p99/p99.9 steps, topology byte/savings metrics, and `topology_analysis()` depth/fanout reports.

### Extend Phase 8 — Wasm query operators (R19)
- [ ] T084: Wasm operator ABI (columnar batches) + data interchange format
- [ ] T085: Compile a restricted plan subset to Wasm (filter/project)
- [ ] T086: Wasm SIMD exploration + perf tests
- [ ] T087: Edge runtime packaging (ship plan artifact)

### Extend Phase 10 — Adaptive row/column execution (R02)
- [ ] T103: Column snapshot cost model (build vs benefit)
- [ ] T104: Query pattern detector for hot projections
- [ ] T105: Dependency-driven incremental refresh/invalidation for snapshots
- [ ] T106: Adaptive controller (online materialization decisions)

### Extend Phase 11 — Intent inference for migration (R17)
- [x] T114: Pattern library for common MySQL idioms (pagination, polling, soft deletes). Evidence: `detect_migration_intents`, `detect_pagination_signal`, `detect_polling_signal`, `detect_soft_delete_signal`, hierarchy/EXISTS/COALESCE detectors, and focused `migration_intent_report_*` tests.
- [x] T115: Sequence-level intent detection (multi-query patterns). Evidence: `detect_polling_signal`, increasing-value correlation in `polling_values`, persisted `intent_history`, `window_ms` filtering, and `migration_intent_report_detects_polling_and_soft_delete`.
- [x] T116: Intent → SkeinQL mapping (cursor API, CDC subscribe, etc.). Evidence: `rewrite_preview_from_suggestion`, `rewrite_snippets_for_intent`, `migration.rewrite_preview`, and rewrite tests for pagination, EXISTS, self-join hierarchy, and recursive CTEs.
- [x] T117: SkeinAdmin “Migration assistant” page. Evidence: the Migration (R17) panel wires `migration.intent_report`, `migration.rewrite_preview`, and `migration.report_export`, renders rewrite cards, and exports migration reports from `web/skeinadmin/src/main.js`.
- [x] T118: Offline report exporter (JSON + markdown). Evidence: `migration.report_export`, `MigrationReportExportResult.report_json`, Markdown rendering in `migration_report_markdown`, and `migration_report_export_contains_json_and_markdown`.

### Extend Phase 18 — Index synthesis from dependency analysis (R16)
- [ ] T175: Dependency capture: predicate columns + range shapes + order-by needs
- [ ] T176: Candidate generator for covering + composite indexes (from deps)
- [ ] T177: Cost/benefit model includes write overhead + compaction overhead
- [ ] T178: Index retirement (unused) + safety rules
- [ ] T179: Evaluation harness: adaptation after workload shifts

### Extend Phase 19 — Edge replay bundles + performance replay (R14, R18)
- [ ] T185: Replay bundle redaction policies (privacy-safe export)
- [ ] T186: Geo-distributed “bundle windows” + routing rules (bounded staleness)
- [ ] T187: Performance bundle extensions (LSM state, cache warm hints, timing annotations)
- [ ] T188: Deterministic performance replay runner + variance report
- [ ] T189: Regression CI harness: compare latency distributions across commits

### Extend Phase 21 — Energy-aware compaction (R20)
- [x] T204: Energy model instrumentation (CPU + IO estimate; optional external signals). Evidence: `CompactionEnergyConfig`, `CompactionEnergyRuntime`, `estimate_compaction_energy`, and `stats.snapshot.compaction.scheduler.energy`.
- [x] T205: Constrained scheduler (energy minimization subject to latency/space bounds). Evidence: `CompactionPolicyKind::EnergyAware`, slack/constraint scoring in `collect_compaction_runtime`, and safe-mode override preserving hard L0 limits.
- [x] T206: External signal integration (battery/plugged, time-of-use pricing hooks). Evidence: `maintenance.compaction.set_policy` accepts `external_signals`, persists `compaction.energy.*`, and SkeinAdmin exposes power/price/carbon controls.
- [x] T207: Evaluation harness: energy vs p99 latency tradeoffs. Evidence: `eval/compaction_scheduler_dashboard.py` compares `energy_aware` with fixed/workload policies and emits energy score plus p99 latency summaries.

### Extend Phase 22 — LLM-assisted semantic autoparameterization (R11)
- [ ] T215: Label schema for “semantic constants” vs parameterizable literals
- [ ] T216: Pluggable classifier interface (offline model first)
- [ ] T217: Feedback loop: cache misses trigger reclassification
- [ ] T218: Metrics: plan-cache hit rate vs classifier overhead
