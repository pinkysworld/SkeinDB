# SkeinDB Research Agenda (Adapted)

This file adapts the attached SkeinDB Research Agenda into the repository as implementable specs and backlog items.

## Executive Summary

This document presents twenty detailed research proposals that extend the SkeinDB architecture. These proposals build on SkeinDB's core innovations: the ValueID-first storage model, SkeinQL native API, dependency tracking for cache coherency, hash-chained WAL, and WebAssembly extensibility. Each proposal includes problem statement, research hypotheses, methodology, evaluation plan, expected contributions, and connections to related work.
The proposals span seven research areas: Storage and Query Optimization (learned indexes, adaptive hybrid execution, delta-chain topology), Security and Privacy (differential privacy, oblivious execution, forensic queries), Web-Native and Modern Applications (merge functions, incremental views, QUIC protocols), AI/ML Integration (vector embeddings, LLM-assisted parameterization, natural language interfaces), Consistency and Distribution (causal ETags, edge replication, schema evolution), Developer Experience and Tooling (index synthesis, intent inference, performance replay), and Systems Research (WebAssembly operators, energy-aware scheduling).
Priority recommendations are based on novelty, feasibility, alignment with SkeinDB architecture, and potential impact. The highest-priority proposals are: Incremental View Maintenance via Dependency Graphs, Causal Consistency via ETag Chains, Differentially Private Aggregate Queries, and Vector Embeddings as First-Class ValueIDs.

## Proposals (20)

- **1. Learned Index Structures for ValueID Lookup** → `docs/research_agenda/R01_learned-index-structures-for-valueid-lookup.md`
- **2. Adaptive Row-Column Hybrid Execution** → `docs/research_agenda/R02_adaptive-row-column-hybrid-execution.md`
- **3. Delta-Chain Topology Optimization** → `docs/research_agenda/R03_delta-chain-topology-optimization.md`
- **4. Differentially Private Aggregate Queries via SkeinQL** → `docs/research_agenda/R04_differentially-private-aggregate-queries-via-skeinql.md`
- **5. Oblivious Query Execution for Multi-Tenant Deployments** → `docs/research_agenda/R05_oblivious-query-execution-for-multi-tenant-deployments.md`
- **6. Forensic Query Language for Hash-Chained WAL** → `docs/research_agenda/R06_forensic-query-language-for-hash-chained-wal.md`
- **7. Optimistic Concurrency with Client-Side Merge Functions** → `docs/research_agenda/R07_optimistic-concurrency-with-client-side-merge-functions.md`
- **8. Incremental View Maintenance via Dependency Graphs** → `docs/research_agenda/R08_incremental-view-maintenance-via-dependency-graphs.md`
- **9. HTTP/3 and QUIC-Native Database Protocol** → `docs/research_agenda/R09_http-3-and-quic-native-database-protocol.md`
- **10. Vector Embeddings as First-Class ValueIDs** → `docs/research_agenda/R10_vector-embeddings-as-first-class-valueids.md`
- **11. LLM-Assisted Query Autoparameterization** → `docs/research_agenda/R11_llm-assisted-query-autoparameterization.md`
- **12. Natural Language to SkeinQL with Verification** → `docs/research_agenda/R12_natural-language-to-skeinql-with-verification.md`
- **13. Causal Consistency via ETag Chains** → `docs/research_agenda/R13_causal-consistency-via-etag-chains.md`
- **14. Geo-Distributed Replay Bundles for Edge Caching** → `docs/research_agenda/R14_geo-distributed-replay-bundles-for-edge-caching.md`
- **15. Conflict-Free Schema Evolution** → `docs/research_agenda/R15_conflict-free-schema-evolution.md`
- **16. Automatic Index Synthesis from Dependency Analysis** → `docs/research_agenda/R16_automatic-index-synthesis-from-dependency-analysis.md`
- **17. Query Intent Inference for Compatibility Migration** → `docs/research_agenda/R17_query-intent-inference-for-compatibility-migration.md`
- **18. Reproducible Performance Regression Testing** → `docs/research_agenda/R18_reproducible-performance-regression-testing.md`
- **19. WebAssembly-Native Query Operators** → `docs/research_agenda/R19_webassembly-native-query-operators.md`
- **20. Energy-Aware Compaction Scheduling** → `docs/research_agenda/R20_energy-aware-compaction-scheduling.md`

## Implementation status snapshot (2026-05-15)

All 20 agenda tracks now have executable prototype coverage in SkeinDB (method surfaces, runtime features, tests, or benchmark scaffolds). The matrix below points to the primary implementation entry points.

| ID | Status | Primary implementation surface |
|---|---|---|
| R01 | Hardened | Learned index hybrid read path, refresh policy, benchmark quantiles, distribution-shift tests (`docs/LEARNED_INDEXES.md`, `crates/skeindb-core/tests/valuestore.rs`) |
| R02 | Implemented (prototype) | Hybrid row+column snapshot surfaces (`docs/COLUMN_SNAPSHOTS.md`, engine snapshot paths) |
| R03 | Hardened | Delta-chained values, periodic snapshots, skip patches, compaction restructuring, topology reports, and delta benchmarks (`docs/DELTA_VALUES.md`, `valuestore` delta tests) |
| R04 | Implemented | Differential privacy RPC family (`dp.*`) + budget tests |
| R05 | Hardened | Oblivious policy/explain/evaluate (`oblivious.*`), padded scans, dummy lookups, trace leakage/overhead reports, and SkeinAdmin controls |
| R06 | Hardened | Forensic WAL query/verify/export (`forensic.*`), SkeinForensic JSON filters, boundary/checkpoint/Merkle inclusion proofs, export bundles, incident-timeline test coverage, and SkeinAdmin Forensics wiring |
| R07 | Hardened | Merge policies, values-only Wasm execution/cancellation, `merge.evaluate`, offline queue docs, and SkeinAdmin Merge & CRDT controls (`docs/MERGE_FUNCTIONS.md`, `merge.*`) |
| R08 | Hardened | Incremental view APIs (`view.create/drop/refresh/evaluate/status/explain_deps`), dependency metadata, auto full-refresh fallback, oracle/benchmark tests, and admin/catalog wiring |
| R09 | Hardened | SkeinQL-over-QUIC framing, Quinn listener, prepared-query streams, 0-RTT write rejection, rebind/multi-stream tests, and `skeindb transport-bench` comparative p99 benchmarking against HTTP/2 and MySQL/TCP |
| R10 | Implemented (prototype) | Vector ingest/search/index status (`vector.*`) + ANN tests |
| R11 | Implemented (prototype) | Autoparameterization analysis/classification (`ai.autoparam.*`) |
| R12 | Hardened | Natural language translate/explain/execute flow with approval-gated verification and NL eval harness (`ai.nl.*`, `skeindb nl-eval`) |
| R13 | Implemented (prototype) | ETag validators + min-causality controls (`query.select` + `If-None-Match`) |
| R14 | Implemented (prototype) | Replay bundle/time-travel export surfaces (`docs/TIME_TRAVEL_REPLAY.md`) |
| R15 | Implemented (prototype) | Conflict-aware schema evolution (`schema.propose_change`, merge/apply APIs, MVCC row schema-version tags) |
| R16 | Implemented (prototype) | Index advisor synthesis/apply/history (`advisor.*`) |
| R17 | Hardened | Compatibility intent inference, rewrite preview, and JSON/Markdown report export (`migration.*`) |
| R18 | Implemented (prototype) | Reproducible replay/report tooling (`docs/TIME_TRAVEL_REPLAY.md`) |
| R19 | Implemented (prototype) | Wasm-native plan compile/run + batch ABI (`wasm.plan.*`) |
| R20 | Hardened | Energy-aware compaction policy, external signals, energy estimates, SkeinAdmin controls, and energy-vs-p99 evaluation harness (`docs/COMPACTION_SCHEDULER.md`, `eval/compaction_scheduler_dashboard.py`) |

## Priority Matrix (from agenda)

| ID | Proposal | Novelty | Feasibility | Alignment | Impact | Priority |
|---|---|---|---|---|---|---|
| 8 | Incremental View Maintenance | High | High | Very High | High | P0 |
| 13 | Causal Consistency via ETags | Very High | Medium | Very High | High | P0 |
| 4 | Differentially Private Queries | High | High | High | High | P0 |
| 10 | Vector Embeddings as ValueIDs | High | Medium | High | Very High | P0 |
| 3 | Delta-Chain Topology | High | High | Very High | Medium | P1 |
| 6 | Forensic Query Language | Very High | Medium | High | Medium | P1 |
| 7 | Client-Side Merge Functions | Medium | High | Very High | High | P1 |
| 16 | Automatic Index Synthesis | Medium | High | High | High | P1 |
| 1 | Learned Indexes for ValueID | High | Medium | Medium | Medium | P2 |
| 9 | HTTP/3 QUIC Protocol | Very High | Low | High | Medium | P2 |
| 14 | Geo-Distributed Replay Bundles | High | Medium | High | Medium | P2 |
| 19 | WebAssembly Query Operators | High | Medium | Medium | Medium | P2 |

# Cross-Cutting Themes
## Dependency Tracking as Infrastructure
SkeinDB's dependency tracking (originally for ETag-based cache validation) emerges as foundational infrastructure for multiple research directions. It enables: incremental view maintenance (#8), causal consistency (#13), CDC changefeeds (original paper), automatic index synthesis (#16), and query intent inference (#17). Investing in rich, efficient dependency tracking pays dividends across the research agenda.
## WebAssembly as Universal Sandbox
WebAssembly appears in multiple proposals: merge functions (#7), query operators (#19), and the original paper's extensions. A robust Wasm runtime with well-designed capability model becomes a platform for extensibility across security boundaries, from untrusted user code to trusted system operators that can run anywhere.
## The Replay Bundle Abstraction
Replay bundles (time-travel debugging in the original paper) generalize to multiple use cases: performance regression testing (#18), edge replication (#14), and forensic analysis (#6). Defining a rich replay bundle format with versioned schema, optional annotations, and cryptographic verification creates a powerful abstraction for data portability.
## AI/ML Integration Points
Several proposals integrate machine learning: learned indexes (#1), LLM-assisted parameterization (#11), natural language interfaces (#12), and workload prediction for scheduling (#3, #20). Designing clean integration points for ML models (training data collection, inference APIs, feedback loops) enables future AI-native database features.
## Privacy as First-Class Concern
Privacy appears explicitly (differential privacy #4, oblivious execution #5) and implicitly (replay bundle redaction, multi-tenant isolation). Building privacy primitives into the storage and query layers, rather than retrofitting them, enables stronger guarantees with lower overhead.
## Protocol Evolution
SkeinDB already supports MySQL protocol and HTTP. Proposals suggest QUIC (#9), causal ETags (#13), and Wasm-based query shipping (#19). Designing for protocol evolution (versioning, negotiation, graceful degradation) enables the system to adapt to changing network and client landscapes.

# Conclusion and Next Steps
This research agenda presents twenty distinct but interconnected directions for extending SkeinDB. The proposals leverage SkeinDB's unique architectural decisions: content-addressed storage via ValueIDs, native SkeinQL API separate from SQL compatibility, dependency tracking for cache coherency, hash-chained WAL for auditability, and WebAssembly for safe extensibility.
Recommended starting points for research teams:
- For systems researchers: Delta-Chain Topology (#3) and WebAssembly Query Operators (#19) offer clean formal problems with measurable outcomes.
- For security researchers: Differential Privacy (#4) and Forensic Query Language (#6) address timely concerns with novel approaches.
- For web/distributed systems researchers: Causal ETags (#13) and HTTP/3 Protocol (#9) reimagine database-web integration.
- For ML researchers: Vector Embeddings (#10) and LLM-Assisted Parameterization (#11) explore AI-database synergies.
- For database practitioners: Incremental View Maintenance (#8) and Automatic Index Synthesis (#16) deliver immediate practical value.
Each proposal is designed to produce publishable research contributions while advancing SkeinDB's capabilities. The cross-cutting themes suggest that investments in core infrastructure (dependency tracking, Wasm runtime, replay bundles) will accelerate multiple research directions simultaneously.

## Repository mapping

See `docs/RESEARCH_BACKLOG.md` for task IDs and how each proposal maps onto the existing Phase A–G implementation plan.
