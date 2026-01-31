# R19 — WebAssembly-Native Query Operators

**Area:** Systems Research

## Problem Statement

SkeinDB proposes WebAssembly for extensions, but what about core query operators? Compiling query plans to WebAssembly could enable execution on edge nodes, in browsers, or on constrained devices while maintaining sandboxing guarantees. This extends SkeinDB's single-binary philosophy to 'the query plan is the portable artifact.'

## Research Hypotheses

- **H1:** Query operators compiled to WebAssembly achieve performance within 2x of native code while providing memory safety and portability.
- **H2:** Wasm query operators can execute in browser environments, enabling client-side query evaluation for cached data.
- **H3:** Wasm compilation enables query shipping to edge nodes, reducing data transfer for selective queries.

## Methodology

- Phase 1 - Operator Library: Implement core operators (scan, filter, project, join, aggregate) in Rust, compiled to WebAssembly. Define stable ABI for data exchange.
- Phase 2 - Query Compilation: Implement query plan to Wasm compilation: (a) operator selection, (b) operator fusion where beneficial, (c) serialization for transmission.
- Phase 3 - Runtime Environments: Build runtimes for: (a) server-side (Wasmtime), (b) browser (native Wasm), (c) edge nodes (Wasm on CDN workers).
- Phase 4 - Optimization: Implement Wasm-specific optimizations: (a) SIMD utilization, (b) memory layout for cache efficiency, (c) streaming evaluation for large results.

## Evaluation Plan

- **E1:** Performance comparison: Wasm operators vs. native operators.
- **E2:** Browser execution: latency for client-side query evaluation.
- **E3:** Edge deployment: query shipping bandwidth savings vs. data shipping.
- **E4:** Portability: same query binary running across server/browser/edge.
- **E5:** TPC-H subset on Wasm runtime.

## Expected Contributions

- WebAssembly compilation target for database query operators.
- Portable query execution across server, browser, and edge.
- Query shipping framework based on Wasm query plans.
- Performance analysis of Wasm for data-intensive operations.

## Key Related Work

- Neumann 'Efficiently Compiling Efficient Query Plans' (2011); Haas et al. 'Bringing the Web up to Speed with WebAssembly' (2017)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
