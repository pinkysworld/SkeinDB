# Research Backlog (Adapted from January 2026 agenda)

This backlog turns the **20 research proposals** in `docs/RESEARCH_AGENDA.md` into **Codex-friendly, PR-sized tasks**, mapped onto the existing Phase A–G build plan and the current `docs/PROJECT_BACKLOG.md` task numbering.

Notes:
- These items are **research-oriented**: the goal is to make each direction implementable and measurable.
- Tasks are designed to be **optional** and do not block core MySQL compatibility.

## Reality sync (2026-05-27)

This file is the research task inventory. It is not the best place to read current maturity at a glance.

- Runtime truth: all R01-R20 tracks have executable coverage in code, methods, tests, or benchmark scaffolds.
- Current maturity split: **R01-R17 and R20 are hardened**; **R18 and R19 remain prototype implemented**.
- Checklist state: **109 done / 0 open** research checkboxes. The checklist is complete, but some tracks still remain prototype-strength in runtime maturity.
- Current source of truth for implemented-vs-partial status: `docs/TRUE_STATUS_MATRIX.md`.

## Current partial research areas

| Track | Truth today | Remaining gap |
|---|---|---|
| R18 Perf regression replay | Replay bundles can carry performance profiles, deterministic replay can rehydrate cache hints, variance reports exist; 2026-06-11 micro: timing injection primitive (`inject_replay_timing`) implemented+exercised in replay run + LSM stats re-compute for fidelity + new unit test `replay_timing_injection_simulates_deterministic_pacing` + test enhancements (engine::tests + cluster). | Full timing pacing integration into runner execution + .github CI distribution gates (e.g. replay-compare step on bundles) remain. |
| R19 Wasm query operators | `wasm.plan.compile/run/inspect/perf_report/edge_package` exists with generated fixed-width artifacts and host fallback. | Production SIMD-lowered codegen and broader hardened operator coverage are not yet claimed. 2026-06-11 micro: hardened host fallback surface coverage (wasm_plan_run dispatch + inspect exercised on host_interpreted_v1 in engine::tests::wasm_plan_compile_falls_back_for_unsupported_types with data roundtrip). See TRUE_STATUS_MATRIX. |

## Recent verified closures
- **2026-06-11:** R19 host fallback micro (see TRUE_STATUS for details).


- **2026-06-11:** R18 micro-slice (timing injection + fidelity): `inject_replay_timing` (R18 primitive for deterministic delay sim from profile) added + exercised in maintenance_replay_run (with LSM stats_snapshot for recon fidelity) + dedicated unit test `engine::tests::replay_timing_injection_simulates_deterministic_pacing` + coverage/asserts in roundtrip + rehydrate tests. Refs in matrix. Full AGENTS followed (fmt/clippy/test relevant, updates, site, review, commit ref review/matrix/R18). Still prototype.
- **2026-06-11:** R19 micro hardening (T085/T086 surfaces): added explicit host fallback `wasm_plan_inspect` + `wasm_plan_run` + result parity assertions inside existing fallback test (exercises the host dispatch in engine.rs:wasm_plan_run for non-generated artifacts). Evidence locked to `engine::tests::wasm_plan_compile_falls_back_for_unsupported_types`. Matrix + this backlog updated. Followed full AGENTS process (fmt/clippy/test/review/site rebuild/commit ref R19/matrix).
- **2026-05-17:** R02 adaptive row/column hardening was finished across T103-T106, covering live-row-count cost modeling, hot-projection tracking, dependency-driven snapshot invalidation, and adaptive replacement decisions.
- **2026-05-17:** R14 bounded-staleness bundle windows were closed with retained coverage-gap detection in `edge.bundle.status`.
- **2026-05-17:** R16 workload-shift evaluation was closed with phased convergence reporting in `advisor.evaluate`.
- **2026-05-17:** R18 deterministic replay reconstruction was closed with replay cache-hint rehydration and normalized replay-run checksums.

Use the task sections below for the detailed per-track inventory and evidence notes.

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
- [x] T240: Add SkeinQL aggregate nodes (COUNT/SUM/AVG) with explicit DP parameters (experimental). Evidence: `dp.aggregate`, `DpAggregateSpec`, COUNT/SUM/AVG result columns, explicit epsilon/delta/mechanism/principal/seed fields, and `dp_budget_consumption_and_exhaustion` / `dp_aggregate_deterministic_noise` tests.
- [x] T241: Sensitivity analysis for single-table aggregates (bounded domains). Evidence: `resolve_dp_aggregates`, bounded `DpBounds` range sensitivities for SUM/AVG/percentile, count sensitivity 1.0, privacy metadata per aggregate, and focused assertions in `dp_budget_consumption_and_exhaustion`.
- [x] T242: Privacy budget manager (per user/role) + persistence. Evidence: `dp.budget.set`, `dp.budget.get`, `DpBudgetDisk` v2 persistence in `dp_budgets.json`, refresh-window resets, RDP query counts, and restart assertions in `dp_budget_consumption_and_exhaustion`.
- [x] T243: Noise mechanisms (Laplace / Gaussian policy) + deterministic tests (seeded RNG). Evidence: `DpRng`, `dp_laplace_noise`, `dp_gaussian_noise`, mechanism validation, seeded deterministic Laplace/Gaussian coverage, and `r04_dp_rng_deterministic_and_uniform` / `r04_dp_laplace_noise_has_correct_scale` / `dp_aggregate_deterministic_noise` tests.
- [x] T244: Privacy-aware caching rules (ETag includes privacy metadata). Evidence: `privacy_etag` in `dp.aggregate` privacy output, derived from a v1 DP validator payload containing table version, query fingerprint, epsilon/delta, mechanism, principal, seed, and budget metadata; locked by `dp_budget_consumption_and_exhaustion`.
- [x] T245: Audit log entries for DP queries (budget consumption). Evidence: `DpAuditEvent`, `dp.audit.log`, persisted `dp_audit.json`, budget remaining epsilon/delta in events, usage summaries in `dp.budget.get`, and restart assertions in `dp_budget_consumption_and_exhaustion`.
- [x] T246: Evaluation harness: accuracy vs epsilon, overhead vs baseline. Evidence: `dp.evaluate`, `DpEvaluateParams` / `DpEvaluateResult`, exact baseline rows, seeded epsilon-grid trials, mean/p95/max absolute error, mean relative error, noisy latency, overhead-vs-exact metrics, SkeinAdmin Privacy controls, and `dp_evaluate_reports_accuracy_and_overhead` / `skeinadmin_privacy_panel_exposes_dp_evaluation_harness` tests.

### Phase 26 — Oblivious query execution (R05)
- [x] T250: Threat model doc + “obliviousness levels” policy schema. Evidence: `docs/OBLIVIOUS_EXECUTION.md`, `ObliviousPolicy`, `oblivious.policy.set`, `normalize_oblivious_policy`, and persisted `ObliviousPolicyDisk` v1.
- [x] T251: ValueStore lookup padding + dummy reads (table/column policy). Evidence: `oblivious_padding_for`, `oblivious_dummy_lookups`, `compute_oblivious_padding`, and `oblivious_scan_keeps_results`.
- [x] T252: Oblivious scan primitive (fixed-size batches, padding). Evidence: `scan_table` applies deterministic padding/shuffle before returning real rows unchanged, locked by `oblivious_policy_explain_padding` and `oblivious_scan_keeps_results`.
- [x] T253: Oblivious sort/join primitive (limited scope, research mode). Evidence: `oblivious.explain` / `oblivious.evaluate` report `materialize_then_sort_join` for padded policies and expose target/dummy access envelopes for fixed-size inputs.
- [x] T254: Leakage evaluation harness (trace-based, mutual information metrics). Evidence: `oblivious.evaluate`, `ObliviousEvaluateResult`, empirical mutual-information metrics, and engine/RPC assertions comparing padded vs unpadded traces.
- [x] T255: Performance overhead report generator. Evidence: `oblivious.evaluate` performance payload with mean/max overhead ratio, total dummy rows/lookups, total observed accesses, and integration coverage in `r05_oblivious_padding_verification`.
- [x] T256: Admin UI settings for per-table obliviousness levels. Evidence: SkeinAdmin Privacy R05 controls for level/pad/target/dummy/shuffle/trace rows, `oblEvaluate()`, and `skeinadmin_privacy_panel_exposes_dp_evaluation_harness` asset coverage.

### Phase 27 — Forensic query language (R06)
- [x] T260: Define SkeinForensic query grammar (minimal) + JSON form over SkeinQL. Evidence: `ForensicQueryParams.filter`, `forensic_filter_matches`, operators `and/or/not/eq/ne/gt/ge/lt/le/contains`, typed-literal operands, field equality shorthand, docs in `docs/AUDIT_WAL.md`, and focused engine/RPC tests.
- [x] T261: Build verifiable WAL index (time/table/user) consistent with hash chain. Evidence: `forensic_index_summary` emits timestamp/id ranges, `by_table`, `by_op`, and `by_actor` buckets tied to the returned chain/proof; actor remains `unknown` until authenticated principal metadata is recorded.
- [x] T262: Proof format for inclusion + boundary proofs; verifier tool. Evidence: `skein.forensic.proof.v1`, boundary `preceding_hash`/`following_hash`, `forensic_merkle_root`, `forensic_merkle_proof`, per-record `inclusion_proofs`, and `forensic.verify` tamper detection.
- [x] T263: `forensic.query` SkeinQL endpoint + exportable report bundles. Evidence: JSON-RPC dispatch, capability advertising, `skein.forensic.bundle.v1` query manifest/proof/verification export shape, and RPC roundtrip coverage.
- [x] T264: Incremental verification via checkpoint anchors. Evidence: persisted `CheckpointAnchor` records and proof fields `checkpoint_anchor`, `next_checkpoint_anchor`, and `anchor_count`, with engine coverage after `checkpoint_for_shutdown()`.
- [x] T265: Case-study harness: simulated incident timelines + proofs. Evidence: `forensic_case_study_exports_incident_timeline` covers non-contiguous filtered incident timelines, inclusion proofs, and export-bundle verification strategy.
- [x] T266: SkeinAdmin “Forensics” page (query + verify + export). Evidence: DB/table/op/id/bundle/filter controls, `readForensicParams`, proof verify now queries then calls `forensic.verify` with returned records/start hash, export includes bundle/filter params, and static asset coverage.

### Phase 28 — Merge functions for optimistic concurrency (R07)
- [x] T270: Conflict model (write-write, constraint, dependency) + detection hooks. Evidence: `merge.apply` handles `expected_etag`, `min_causality`, primary-key mismatch, and non-null constraint failures; focused tests cover conflict and non-null rejection paths.
- [x] T271: Merge function registry (Wasm) + capability model ("values-only" access). Evidence: `merge.wasm.*`, persisted `merge_wasm_registry.json` v1, `validate_merge_wasm_policy`, and executable values-only scalar Wasm merge modules.
- [x] T272: SkeinQL `merge.register` / `merge.apply` + SQL compat hook (If-Match). Evidence: typed SkeinQL params/results, RPC dispatch/capability advertising, ETag/min-causality merge guards, and `merge_apply_wasm_policy_executes_rpc`.
- [x] T273: Offline write queue format (client SDK spec) + merge result handling. Evidence: `docs/OFFLINE_WRITE_QUEUE.md` plus `crates/skeindb-skeinql/tests/offline_queue_roundtrip.rs`.
- [x] T274: Safety tests: cancellation + deterministic merges. Evidence: `merge_apply_wasm_policy_cancels_non_terminating_module`, `merge_apply_wasm_policy_executes_values_only_module`, and `cluster_rpc.rs::r07_merge_conflict_resolution_deterministic`.
- [x] T275: Bench: conflict rate + resolution success on example workloads. Evidence: read-only `merge.evaluate` returns `skein.merge.evaluate.v1` with conflict/resolution rates, mean/p95 timing, and per-case results.
- [x] T276: SkeinAdmin "Merge rules" page. Evidence: Merge & CRDT panel now sends typed apply/register/simulate/evaluate/Wasm payloads and `skeinadmin_merge_panel_exposes_r07_hardening_controls` locks the controls.

### Phase 29 — Incremental view maintenance (R08)
- [x] T280: `view.create` SkeinQL method with persisted definition (SkeinIR). Evidence: `view.create`, `views.json` format v2, restart persistence, and `view_dependency_usage_persists_in_views_json`.
- [x] T281: Dependency graph extension: view → base table deps at column granularity. Evidence: dependency objects include `columns`, `projection_columns`, `predicate_columns`, `group_by_columns`, and `view.explain_deps` traverses direct/transitive graph edges.
- [x] T282: Delta derivation for a restricted operator set (filter, project, group-by). Evidence: restricted single-table filter/project views, grouped aggregate plans for grouped columns plus `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, and grouped source-row persistence.
- [x] T283: Incremental refresh pipeline (apply deltas from CDC stream). Evidence: `refresh_view_incremental`, `refresh_grouped_view_incremental`, change-log driven stale marking, touched-row/touched-group recompute, and restart coverage.
- [x] T284: Cost-based switch: incremental vs full recompute. Evidence: `view.refresh` `mode:"auto"`, `view_refresh_stats`, `should_refresh_view_full`, and `view_grouped_auto_refresh_prefers_full_for_wide_change_sets`.
- [x] T285: Correctness oracle: compare incremental vs recompute on random workloads. Evidence: `view.evaluate` compares cloned incremental and full refresh results and `r08_view_correctness_oracle_random_workload_matches_full_recompute` runs a deterministic pseudo-random workload.
- [x] T286: Bench: view maintenance overhead + query speedups. Evidence: `view.evaluate` returns mean incremental/full nanoseconds, speedup-vs-full, pending changes, touched primary keys, and recommended mode.
- [x] T287: SkeinAdmin “Views” page (status, refresh, explain deps). Evidence: Views panel now exposes refresh mode, evaluate iterations, `view.evaluate`, status, drop, dependency summaries, RPC templates, and static asset coverage.

### Phase 30 — HTTP/3 / QUIC-native protocol (R09)
- [x] T290: Protocol sketch: SkeinQL-over-QUIC framing + stream mapping. Evidence: `docs/TRANSPORT_QUIC.md` defines length-prefixed JSON frames, one request/response per bidirectional stream, envelope metadata, and stream mapping.
- [x] T291: Implement server prototype with a QUIC library (feature-flag). Evidence: `skeindb serve --quic --quic-cert --quic-key`, Quinn integration, `transport.capabilities`, and `quic_rpc_ping_roundtrip`.
- [x] T292: Prepared query handles over QUIC streams (read-only first). Evidence: `quic_prepared_query_roundtrip` prepares a query, executes it on a new QUIC stream, and verifies result rows.
- [x] T293: 0-RTT safety rules (no writes in 0-RTT by default). Evidence: QUIC metadata `rtt:"0rtt"` is documented and `quic_zero_rtt_rejects_write` locks the read-only guard.
- [x] T294: Bench: p99 latency under concurrency vs HTTP/2 and MySQL/TCP. Evidence: `skeindb transport-bench`, `transport_bench_parses_flags`, `latency_stats_computes_percentiles`, and `transport_bench_reports_http2_quic_and_mysql`.
- [x] T295: Connection migration test harness (simulated IP change). Evidence: `quic_connection_migration_rebind` and `r09_quic_concurrent_multi_stream_rpcs` rebind the client UDP socket and verify continued RPC success.

### Phase 31 — Vector embeddings as first-class ValueIDs (R10)
- [x] T300: Add `ValueKind::Embedding` and typed literal support in SkeinQL. Evidence: `ValueKind::Embedding`, `Lit::Embedding { dims, v, model }`, `embedding_literal_roundtrip`, and embedding persistence through `value_store_item`.
- [x] T301: LSH bucket + content hash ValueID scheme (exact + approximate id). Evidence: `embedding_lsh_bucket`, `embedding_value_id`, `value_store_item` using `ValueKind::Embedding`, and `embedding_value_id_combines_lsh_bucket_and_content_hash`.
- [x] T302: ANN search operator (bucket filter + distance refine) (baseline). Evidence: `vector.search`, HNSW `HnswIndex`, LSH fallback/refinement, `r10_hnsw_index_basic`, `vector_prefilter_candidates_match_bucket`, and `quic_vector_search_roundtrip`.
- [x] T303: Hybrid query: filter predicates + ANN order-by. Evidence: vector filters in `vector.search`, `vector.cosine` / `vector.dot` / `vector.l2` order expressions, and `query_select_orders_by_vector_similarity`.
- [x] T304: Dependency tracking for embedding-derived queries (invalidate on source change). Evidence: `vector.search.cache` returns table-version dependencies, ETags, `not_modified`, and V2 causality tokens; ordinary `data.*` writes invalidate stale HNSW graphs; `vector.insert` emits prepared-query SSE invalidations; covered by `vector_search_cache_metadata_tracks_source_table_changes` and `vector_search_cache_invalidates_after_vector_insert_rpc`.
- [x] T305: Bench harness: recall/latency vs baseline index. Evidence: `vector.benchmark`, `VectorBenchmarkResult` latency percentiles, exact-vs-HNSW recall@k, `vector_benchmark_reports_recall_and_latency`, and `quic_vector_search_roundtrip` live RPC coverage.
- [x] T306: Example app: small RAG retrieval pipeline. Evidence: `samples/vector_rag_pipeline.py` seeds deterministic chunks, uses `vector.insert` / `vector.search`, assembles a grounded prompt, and exposes a no-server `--self-test`; `docs/tutorials/vector-rag.md`, the generated docs guide page, and `sample_assets.rs` / `docs_site_assets.rs` lock the tutorial and sample flow.
- [x] T307: SkeinAdmin “Embeddings” page (index status + query playground). Evidence: the Vector Search panel exposes DB/table/column/vector/PK/filter controls plus Search/Benchmark/Insert/Index Status actions, and `skeinadmin_vectors_panel_exposes_embedding_query_playground` locks the UI wiring.

### Phase 32 — Natural language to SkeinQL with verification (R12)
- [x] T310: NL→SkeinQL prompt+schema packaging format (offline first). Evidence: `AiNlPromptPackage`, `build_nl_prompt`, `ai.nl.translate`, and `ai_nl_translate_packages_schema` / `ai_nl_translate_explain_execute_roundtrip`.
- [x] T311: Query explanation generator from dependency sets + planner info. Evidence: `ai.nl.explain`, `dependencies_for_query`, `ai_nl_preview`, and explanation fields for tables/projection/filters/order/limit/deps.
- [x] T312: Verification UI flow: explanation + sample rows + approval gate. Evidence: SkeinAdmin NL Lab query JSON / preview / approval-token controls plus `ai_nl_translate_explain_execute_rpc_roundtrip`.
- [x] T313: Safety policy: forbid writes unless explicit confirmation token. Evidence: the SkeinQL `Query` shape is read-query-only for this surface, `ai.nl.explain` only accepts SELECT, `ai.nl.execute` recomputes the approval token from query+args+deps, and tampered-query execution is rejected in `ai_nl_translate_explain_execute_roundtrip`.
- [x] T314: Evaluation harness: adapted text-to-SQL benchmarks (execution match). Evidence: `skeindb nl-eval`, `NlEvalReport.execution_matches`, `eval_examples_exact_and_exec_match`, and `eval_examples_uses_rule_translation_for_execution_match`.
- [x] T315: Iterative refinement protocol (user feedback loop). Evidence: prompt packages include stable `fingerprint`, editable generated query JSON, explicit args, preview re-explain, and reapproval before execution in SkeinAdmin.
- [x] T316: SkeinAdmin “NL Query” page (experimental). Evidence: NL Lab (R11-R12) panel wires `ai.nl.translate`, `ai.nl.explain`, and `ai.nl.execute` with approval-token-gated execution.

### Phase 33 — Conflict-free schema evolution (R15)
- [x] T320: Schema version tagging in MVCC row versions. Evidence: `RowEntry.schema_version` / `RowEntryDisk.schema_version`, v3 table-row payloads with legacy v2 normalization on load, merge/apply row stamping, and `schema_version_tags_row_entries_and_normalizes_legacy_rows`.
- [x] T321: Concurrent schema changes protocol (add column/index) + conflict detection
- [x] T322: Query execution across schema heterogeneity (safe conversions). Evidence: schema-aware row materialization now adapts heterogeneous legacy rows via MySQL-compatible column defaults or `NULL` across batch/non-batch selects, keyed reads, joins, and `query_select_adapts_legacy_rows_to_schema_defaults`.
- [x] T323: Schema merge algorithm + roll-forward/rollback rules. Evidence: `schema.apply_merge` now rolls forward eligible proposals, marks deterministic losers `rejected`, returns `rolled_back` conflict details, and clears resolved losers from later `schema.merge_status` output in `schema_evolution_merges_concurrent_column_and_index_changes` and `r15_schema_evolution_concurrent_column_and_index_changes`.
- [x] T324: Migration assistant: show divergence + propose resolution. Evidence: `schema.merge_status` now returns a structured `resolution` plan with `roll_forward`, `rollback`, and `wait` actions plus caller-facing suggestions in `schema_merge_status_proposes_resolution_actions` and `r15_schema_evolution_concurrent_column_and_index_changes`.
- [x] T325: Rolling-deploy simulation harness. Evidence: `schema.simulate_rollout` now projects prepare/mixed/steady-state rollout stages, per-wave upgraded/legacy node counts, and legacy-row adaptation notes in `schema_simulate_rollout_reports_mixed_version_waves` and `r15_schema_evolution_concurrent_column_and_index_changes`.
- [x] T326: SkeinAdmin “Schema evolution” page. Evidence: the Schema panel now exposes typed `schema.propose_change`, `schema.merge_status`, `schema.simulate_rollout`, and `schema.apply_merge` controls plus focused asset coverage in `skeinadmin_schema_panel_exposes_r15_evolution_controls`.

## Extensions to existing phases (inline patches)

The following additions extend existing phases in `docs/PROJECT_BACKLOG.md`.

### Extend Phase 6 — Causal ETag chains (R13)
- [x] T064: Define causal ETag format (compressed dependencies / vector-clock hybrid). Evidence: the runtime emits `vector_clock_v2` tokens, `CAUSALITY_FORMAT_V2` is covered by `r13_vector_clock_causality`, and the wire format is documented in `docs/ETAG_VALIDATORS.md` / `docs/SKEINQL.md`.
- [x] T065: `min_causality` request field + response causality propagation rules. Evidence: `query.select`, `query.execute_prepared`, `merge.apply`, and `vector.search` accept `min_causality` and return `causality`; covered by `query_select_min_causality_enforced` and `query_execute_prepared_honors_causal_cache_validators`.
- [x] T066: Replication propagates causality metadata (no total order required). Evidence: replicated writes ship `x-skeindb-replication-causality`, replicas merge the applied watermark into `cluster.status` / `stats.snapshot.cluster.replication`, and `replicated_writes_include_causality_header` plus `cluster_replication_ships_schema_and_rows` lock the behavior.
- [x] T067: Cache interaction tests (If-None-Match with causal validators). Evidence: `query_select_min_causality_enforced` and `query_execute_prepared_honors_causal_cache_validators` verify `if_none_match` + `min_causality` on satisfied and unsatisfied dependency floors.

### Extend Phase 7 — Delta topology optimization (R03)
- [x] T073: Implement periodic full snapshots for deltas (K-depth policy). Evidence: `DeltaPolicy.snapshot_interval`, `ValueStore::put_with_delta`, and `delta_snapshot_interval_enforces_raw`.
- [x] T074: Skip-pointer (skip-list) delta chain encoding. Evidence: `SkipPatch`, geometric `build_skip_patches`, `materialize_with_trace`, and `skip_patches_reduce_steps`.
- [x] T075: Compaction-time topology restructuring policy. Evidence: `ValueStore::compact_deltas`, `DeltaCompactionReport`, and `delta_compaction_rewrites_deep_chains`.
- [x] T076: Bench: reconstruction latency vs write amplification. Evidence: `ValueStore::delta_benchmark()` p50/p99/p99.9 steps, topology byte/savings metrics, and `topology_analysis()` depth/fanout reports.

### Extend Phase 8 — Wasm query operators (R19)
- [x] T084: Wasm operator ABI (columnar batches) + data interchange format. Evidence: `docs/WASM_OPERATORS.md`, `wasm.plan.compile/run`, `wasm_batch_v1` result format, `wasm.plan.inspect`, and `engine::tests::wasm_plan_compile_and_run`.
- [x] T085: Compile a restricted plan subset to Wasm (filter/project). Evidence: fixed-width non-null `u64`/`bool` filter/project plans now emit `generated_filter_project_v1` artifacts with embedded module metadata, `wasm.plan.run` executes those artifacts through Wasmtime while unsupported plans fall back to `host_interpreted_v1`, and focused coverage lives in `engine::tests::wasm_plan_compile_and_run`, `engine::tests::wasm_plan_compile_falls_back_for_unsupported_types`, `server::tests::wasm_plan_compile_run_rpc`, `server::tests::wasm_plan_run_batch_rpc`, and `tests/quic_rpc.rs::quic_wasm_plan_run_batch`.
- [x] T086: Wasm SIMD exploration + perf tests. Evidence: `wasm.plan.perf_report`, `WasmPlanPerfReportResult`, latency min/p50/p95/p99/mean stats for host and generated execution, output-parity checks, explicit `WasmPlanSimdExploration` candidate/enabled/notes fields, and focused engine/RPC tests. This is an exploration baseline; `supports_simd` remains false until a future production SIMD-lowered codegen path exists.
- [x] T087: Edge runtime packaging (ship plan artifact). Evidence: `wasm.plan.edge_package` now emits `standalone_execution` metadata and a packaged JavaScript runner with `runSkeinWasmPlanEdge` for generated artifacts, local `skein.wasm.batch.v1` encode/decode helpers, `WebAssembly.instantiate` execution for embedded `generated_filter_project_v1` modules, and `runSkeinWasmPlanHost` fallback for `host_interpreted_v1` artifacts.

### Extend Phase 10 — Adaptive row/column execution (R02)
- [x] T103: Column snapshot cost model (build vs benefit). Evidence: `SnapshotManager::next_plan` already compares snapshot build cost against projected row-scan savings, and `observe_and_plan_snapshot` now feeds live table row counts so selective probes do not underestimate full snapshot build cost. Covered by `engine::tests::snapshot_cost_model_uses_live_table_rows_for_selective_queries`.
- [x] T104: Query pattern detector for hot projections. Evidence: `QueryPatternTracker` now retains a bounded per-table hot set of normalized column patterns, and only replaces colder projections when a hotter candidate arrives. Covered by `engine::tests::query_pattern_tracker_retains_hot_projections` and `engine::tests::pattern_tracker_and_cost_model`.
- [x] T105: Dependency-driven incremental refresh/invalidation for snapshots. Evidence: schema-version bumps now preserve unaffected snapshots, column renames rewrite dependent snapshot/pattern metadata, and column drops invalidate only the dependent snapshots/patterns. Covered by `engine::tests::snapshot_dependencies_preserve_unrelated_snapshot_on_drop_column`.
- [x] T106: Adaptive controller (online materialization decisions). Evidence: `SnapshotManager::next_plan` now compares candidate materializations against the best active covering snapshot instead of treating every covering snapshot as a permanent blocker, allowing narrower hot projections to replace broader ones when the online benefit stays positive. Covered by `engine::tests::adaptive_snapshot_controller_replaces_broad_covering_snapshot_when_shifted`.

### Extend Phase 11 — Intent inference for migration (R17)
- [x] T114: Pattern library for common MySQL idioms (pagination, polling, soft deletes). Evidence: `detect_migration_intents`, `detect_pagination_signal`, `detect_polling_signal`, `detect_soft_delete_signal`, hierarchy/EXISTS/COALESCE detectors, and focused `migration_intent_report_*` tests.
- [x] T115: Sequence-level intent detection (multi-query patterns). Evidence: `detect_polling_signal`, increasing-value correlation in `polling_values`, persisted `intent_history`, `window_ms` filtering, and `migration_intent_report_detects_polling_and_soft_delete`.
- [x] T116: Intent → SkeinQL mapping (cursor API, CDC subscribe, etc.). Evidence: `rewrite_preview_from_suggestion`, `rewrite_snippets_for_intent`, `migration.rewrite_preview`, and rewrite tests for pagination, EXISTS, self-join hierarchy, and recursive CTEs.
- [x] T117: SkeinAdmin “Migration assistant” page. Evidence: the Migration (R17) panel wires `migration.intent_report`, `migration.rewrite_preview`, and `migration.report_export`, renders rewrite cards, and exports migration reports from `web/skeinadmin/src/main.js`.
- [x] T118: Offline report exporter (JSON + markdown). Evidence: `migration.report_export`, `MigrationReportExportResult.report_json`, Markdown rendering in `migration_report_markdown`, and `migration_report_export_contains_json_and_markdown`.

### Extend Phase 18 — Index synthesis from dependency analysis (R16)
- [x] T175: Dependency capture: predicate columns + range shapes + order-by needs. Evidence: `AdvisorIndexDependencyCapture` is returned on `advisor.index_synthesize` suggestions with predicate/equality/range/order/group/join/projection columns and `range_shape`; covered by `index_advisor_synthesizes_candidate` and `index_advisor_extracts_group_and_like`.
- [x] T176: Candidate generator for covering + composite indexes (from deps). Evidence: `advisor_candidate_columns_from_dependency` builds equality/join/range/group/order composite keys, `advisor_covering_include_from_dependency` emits projection-derived include columns, and `index_advisor_generates_composite_and_covering_from_dependencies` locks the behavior.
- [x] T177: Cost/benefit model includes write overhead + compaction overhead. Evidence: `AdvisorIndexCostEstimate` reports read benefit, write overhead, compaction overhead, write pressure, key/include width, and net score; covered by `index_advisor_cost_model_includes_write_and_compaction_overhead`.
- [x] T178: Index retirement (unused) + safety rules. Evidence: `advisor.retire_unused`, dry-run/stale-signal safety, action `retire` history entries, latest-action suppression rules, and retirement tests for inactive vs recently used advisor indexes.
- [x] T179: Evaluation harness: adaptation after workload shifts. Evidence: `advisor.evaluate` returns `skein.advisor.evaluate.v1` with phased top-suggestion convergence metrics; covered by `advisor_evaluate_reports_shift_convergence`, `advisor_evaluate_roundtrip_reports_shift_convergence`, and `r16_index_advisor_evaluate_reports_shift_convergence`.

### Extend Phase 19 — Edge replay bundles + performance replay (R14, R18)
- [x] T185: Replay bundle redaction policies (privacy-safe export). Evidence: `MaintenanceReplayExportParams.redaction`, optional `ReplayBundle.redaction`, `hash_pk` / `drop_pk` primary-key redaction before checksums, SkeinAdmin/CLI controls, and replay import/run coverage for redacted bundles.
- [x] T186: Geo-distributed “bundle windows” + routing rules (bounded staleness). Evidence: `edge.bundle.apply` now preserves disjoint coverage windows per table while merging adjacent/overlapping ranges only, and `edge.bundle.status` rejects bounded-staleness routes with reason `coverage_gap` when contiguous coverage is incomplete; covered by `edge_bundle_status_detects_coverage_gap`, `edge_bundle_status_reports_coverage_gap`, and `r14_edge_bundle_gap_blocks_bounded_staleness_route`.
- [x] T187: Performance bundle extensions (LSM state, cache warm hints, timing annotations). Evidence: optional `ReplayBundle.performance`, `ReplayBundlePerformanceProfile`, storage/cache/timing sections, checksum validation, and `replay_bundle_export_import_run_roundtrip`.
- [x] T188: Deterministic performance replay runner + variance report. Evidence: `maintenance.replay.run` now rehydrates captured select/patch cache counts inside the replay workspace, computes replay-run checksum parity over reconstructable snapshot state, and keeps raw disk/WAL/cache/timing deltas in `performance_report`; covered by `replay_bundle_run_rehydrates_cache_hints`, `maintenance_replay_run_rehydrates_cache_hints`, and `t188_replay_run_rehydrates_cache_hints`.
- [x] T189: Regression CI harness: compare latency distributions across commits. Evidence: `skeindb replay run --json --out`, `skeindb replay compare --baseline --candidate`, threshold flags for p95/p99/span/storage/cache-hot-table deltas, JSON comparison reports, non-zero exit on regressions, and focused CLI tests.

### Extend Phase 21 — Energy-aware compaction (R20)
- [x] T204: Energy model instrumentation (CPU + IO estimate; optional external signals). Evidence: `CompactionEnergyConfig`, `CompactionEnergyRuntime`, `estimate_compaction_energy`, and `stats.snapshot.compaction.scheduler.energy`.
- [x] T205: Constrained scheduler (energy minimization subject to latency/space bounds). Evidence: `CompactionPolicyKind::EnergyAware`, slack/constraint scoring in `collect_compaction_runtime`, and safe-mode override preserving hard L0 limits.
- [x] T206: External signal integration (battery/plugged, time-of-use pricing hooks). Evidence: `maintenance.compaction.set_policy` accepts `external_signals`, persists `compaction.energy.*`, and SkeinAdmin exposes power/price/carbon controls.
- [x] T207: Evaluation harness: energy vs p99 latency tradeoffs. Evidence: `eval/compaction_scheduler_dashboard.py` compares `energy_aware` with fixed/workload policies and emits energy score plus p99 latency summaries.

### Extend Phase 22 — LLM-assisted semantic autoparameterization (R11)
- [x] T215: Label schema for “semantic constants” vs parameterizable literals. Evidence: `ai.autoparam.label_schema` exposes `skein.ai.autoparam.label_schema.v1` with literal context fields, label result fields, confidence bounds, and explicit `parameterize`, `semantic_constant`, and `unknown` cache-key policies.
- [x] T216: Pluggable classifier interface (offline model first). Evidence: `ai.autoparam.classifiers` exposes the supported classifier catalog, `offline_rules_v1` is selectable through `classifier` on classify/analyze requests, and unsupported classifier names return an error.
- [x] T217: Feedback loop: cache misses trigger reclassification. Evidence: `ai.autoparam.feedback` accepts `cache_event: "plan_cache_miss"`, re-runs the selected classifier, records `cached_before`/`reclassified`, and accumulates per-fingerprint miss and reclassification counts.
- [x] T218: Metrics: plan-cache hit rate vs classifier overhead. Evidence: `ai.autoparam.metrics` reports plan-cache hit/miss rate together with classifier invocation/latency counters and feedback reclassification totals.
