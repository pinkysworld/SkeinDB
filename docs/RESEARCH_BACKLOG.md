# Research Backlog (Adapted from January 2026 agenda)

This backlog turns the **20 research proposals** in `docs/RESEARCH_AGENDA.md` into **Codex-friendly, PR-sized tasks**, mapped onto the existing Phase A–G build plan and the current `docs/PROJECT_BACKLOG.md` task numbering.

Notes:
- These items are **research-oriented**: the goal is to make each direction implementable and measurable.
- Tasks are designed to be **optional** and do not block core MySQL compatibility.

## Reality sync (2026-02-28)

Runtime status and checklist status intentionally differ:
- Runtime: all R01-R20 tracks have prototype coverage in code/method surfaces/tests.
- Checklist below: remains open for hardening, stronger benchmarks, and publication-grade evaluation.
- This sync does not promote any research checklist items; it only reconfirms that runtime prototype coverage and checklist status still intentionally differ.

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
- [ ] T230: Instrument ValueID lookup distribution + export histograms
- [ ] T231: Prototype learned model index (offline build) with fallback structure
- [ ] T232: Integrate hybrid learned+fallback lookup into ValueStore read path (feature flag)
- [ ] T233: Compaction-time model refresh policy + correctness tests
- [ ] T234: Benchmark harness: lookup p50/p99/p99.9 + memory overhead
- [ ] T235: Distribution shift tests + graceful degradation

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
- [ ] T310: NL→SkeinQL prompt+schema packaging format (offline first)
- [ ] T311: Query explanation generator from dependency sets + planner info
- [ ] T312: Verification UI flow: explanation + sample rows + approval gate
- [ ] T313: Safety policy: forbid writes unless explicit confirmation token
- [ ] T314: Evaluation harness: adapted text-to-SQL benchmarks (execution match)
- [ ] T315: Iterative refinement protocol (user feedback loop)
- [ ] T316: SkeinAdmin “NL Query” page (experimental)

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
- [ ] T073: Implement periodic full snapshots for deltas (K-depth policy)
- [ ] T074: Skip-pointer (skip-list) delta chain encoding
- [ ] T075: Compaction-time topology restructuring policy
- [ ] T076: Bench: reconstruction latency vs write amplification

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
- [ ] T114: Pattern library for common MySQL idioms (pagination, polling, soft deletes)
- [ ] T115: Sequence-level intent detection (multi-query patterns)
- [ ] T116: Intent → SkeinQL mapping (cursor API, CDC subscribe, etc.)
- [ ] T117: SkeinAdmin “Migration assistant” page
- [ ] T118: Offline report exporter (JSON + markdown)

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
- [ ] T204: Energy model instrumentation (CPU + IO estimate; optional external signals)
- [ ] T205: Constrained scheduler (energy minimization subject to latency/space bounds)
- [ ] T206: External signal integration (battery/plugged, time-of-use pricing hooks)
- [ ] T207: Evaluation harness: energy vs p99 latency tradeoffs

### Extend Phase 22 — LLM-assisted semantic autoparameterization (R11)
- [ ] T215: Label schema for “semantic constants” vs parameterizable literals
- [ ] T216: Pluggable classifier interface (offline model first)
- [ ] T217: Feedback loop: cache misses trigger reclassification
- [ ] T218: Metrics: plan-cache hit rate vs classifier overhead
