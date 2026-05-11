# R08 — Incremental View Maintenance via Dependency Graphs

**Area:** Web-Native & Modern Applications

## Problem Statement

SkeinDB's CDC work tracks dependencies for invalidation signals. The natural extension is computing incremental deltas for materialized views rather than just signaling 'invalidated.' This is a classic database problem, but SkeinDB's existing dependency graph structure provides a foundation for efficient incremental maintenance without requiring separate view management infrastructure.

## Research Hypotheses

- **H1:** SkeinDB's query dependency tracking can be extended to compute delta queries that maintain views incrementally.
- **H2:** For common view patterns (aggregates, joins, filters), incremental maintenance is cheaper than recomputation when update batches are small relative to base table size.
- **H3:** The dependency graph can identify cascading view updates and batch them efficiently.

## Methodology

- Phase 1 - Delta Derivation: Implement automatic delta query derivation from view definitions. For a view V = Q(R), derive delta query dV = dQ(R, dR) that computes view changes from base table changes.
- Phase 2 - Dependency Graph Extension: Extend dependency tracking to represent view-base-table relationships. When base tables change, traverse the graph to identify affected views and their delta queries.
- Phase 3 - Cost-Based Switching: Implement a cost model that decides between incremental maintenance and full recomputation based on: (a) delta size, (b) view complexity, (c) staleness tolerance.
- Phase 4 - Cascading Updates: Handle views defined on views. Implement topological ordering of view updates and delta propagation through multiple levels.

## Evaluation Plan

- **E1:** Incremental maintenance speedup vs. full recomputation for various view types and update patterns.
- **E2:** Overhead of maintaining delta computation infrastructure.
- **E3:** Correctness verification: incremental results match full recomputation.
- **E4:** TPC-H derived views under varying update rates.
- **E5:** Comparison with dedicated IVM systems (Materialize, Noria).

## Implementation Status

Status: **Hardened in v0.3.15** for the restricted R08 surface.

SkeinDB now supports native materialized view management through
`view.create`, `view.drop`, `view.refresh`, `view.evaluate`, `view.status`, and
`view.explain_deps`. The hardened scope covers restricted single-table
filter/project views and grouped aggregate views over `COUNT`, `SUM`, `AVG`,
`MIN`, and `MAX`. View definitions persist in `views.json` format v2 with
column-granular dependency metadata. Incremental refresh consumes the change log
and recomputes touched rows or touched groups, while `mode:"auto"` falls back to
full recompute for broad change sets.

`view.evaluate` is the R08 oracle/benchmark report. It compares cloned
incremental refresh against cloned full recompute, verifies row signatures, and
returns pending-change, timing, speedup, and recommended-mode fields without
mutating the live view. SkeinAdmin exposes refresh mode and evaluation controls,
and compatibility catalogs surface native views through MySQL
`information_schema.views` and PostgreSQL `pg_catalog.pg_views`.

## Expected Contributions

- Integration of incremental view maintenance with dependency-tracking CDC infrastructure.
- Cost model for incremental vs. full recomputation in LSM-based systems.
- Automatic delta query derivation for SkeinQL view definitions.
- Unified framework connecting caching, CDC, and materialized views.

## Key Related Work

- Gupta & Mumick 'Maintenance of Materialized Views' (1995); McSherry et al. 'Differential Dataflow' (2013); Gjengset et al. 'Noria' (2018)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
