# R07 — Optimistic Concurrency with Client-Side Merge Functions

**Area:** Web-Native & Modern Applications

## Runtime Status (v0.3.14)

R07 is implemented as a hardened experimental runtime surface:

- `merge.register`, `merge.apply`, and `merge.simulate` support built-in merge functions and policy entries that reference registered values-only Wasm modules.
- `merge.apply` detects write-write conflicts through `expected_etag`, dependency conflicts through `min_causality`, and constraint conflicts through primary-key / non-null validation.
- `merge.wasm.register`, `merge.wasm.list`, and `merge.wasm.drop` persist module metadata and require `capabilities.values_only = true` before policy execution.
- Wasm merge modules execute through the core scalar Wasm sandbox with fuel, memory, output, and wall-clock cancellation limits.
- `merge.evaluate` returns `skein.merge.evaluate.v1` reports with conflict rate, resolution success rate, mean/p95 merge timing, and per-case outcomes.
- SkeinAdmin's Merge & CRDT panel is wired to apply/register/simulate/evaluate flows and Wasm module management.

Current limits remain intentional: Wasm merge modules are scalar values-only functions, not cross-row/table readers, and the evaluation timing is a local runtime signal rather than a publication-grade benchmark.

## Problem Statement

SkeinDB's ETag system handles cache validation for reads, but write conflicts remain challenging for modern applications. Offline-first architectures and collaborative editing require conflict resolution beyond simple last-write-wins. By extending SkeinQL to support client-supplied merge functions, SkeinDB could bridge web-native consistency with application-specific conflict resolution, similar to CRDTs but with more flexibility.

## Research Hypotheses

- **H1:** Many application-level conflicts can be resolved automatically by merge functions that understand domain semantics, reducing the need for manual intervention.
- **H2:** SkeinDB's dependency tracking can identify conflicting writes and invoke appropriate merge functions without requiring application-level conflict detection.
- **H3:** WebAssembly-based merge functions provide sufficient expressiveness while maintaining security isolation.

## Methodology

- Phase 1 - Conflict Model: Formalize conflict detection using SkeinDB's MVCC and dependency tracking. Define conflict types: (a) write-write on same key, (b) read-write dependencies violated, (c) constraint violations.
- Phase 2 - Merge API: Design SkeinQL extensions for registering merge functions: (a) per-table default merge, (b) per-column merge for structured data, (c) type-specific merge (counters, sets, text). Merge functions receive conflicting versions and produce resolved version.
- Phase 3 - Wasm Integration: Implement merge function execution in WebAssembly sandbox. Define capability model: merge functions can read conflicting values and produce output but cannot access other data.
- Phase 4 - Offline Support: Design client-side SDK that: (a) queues writes during offline periods, (b) includes expected version (ETag) with writes, (c) handles merge results on reconnection.

## Evaluation Plan

- **E1:** Conflict resolution success rate across application types (collaborative docs, shopping carts, inventory).
- **E2:** Merge function execution overhead in WebAssembly sandbox.
- **E3:** Developer experience: time to implement domain-specific merge functions.
- **E4:** Comparison with CRDT libraries (Yjs, Automerge) on expressiveness and performance.
- **E5:** Offline scenario testing: reconnection merge correctness and latency.

## Expected Contributions

- Integration of optimistic concurrency with pluggable merge semantics in a database engine.
- WebAssembly-based secure merge function execution model.
- Formalization of conflict detection using dependency tracking.
- Bridge between database consistency and offline-first architectures.

## Key Related Work

- Shapiro et al. 'CRDTs' (2011); Kleppmann & Beresford 'Automerge' (2017); DeCandia et al. 'Dynamo' (2007)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
