# R14 — Geo-Distributed Replay Bundles for Edge Caching

**Area:** Consistency & Distribution

## Problem Statement

SkeinDB's replay bundles are designed for debugging, but they're also a replication primitive. For edge deployments, nodes could maintain partial replicas via bounded WAL slices rather than full database copies. This creates a continuum between full replicas (consistent but expensive) and CDN caching (cheap but limited), with replay bundles providing configurable consistency-latency tradeoffs.

## Research Hypotheses

- **H1:** Replay bundles can serve as a partial replication primitive, where edge nodes maintain bounded WAL windows for frequently accessed data.
- **H2:** Query routing can intelligently direct queries to edge nodes that have sufficient WAL coverage, with fallback to origin for uncovered data.
- **H3:** Adaptive bundle sizing (based on query patterns and edge capacity) can optimize the consistency-latency-cost tradeoff.

## Methodology

- Phase 1 - Edge Bundle Protocol: Design protocol for edge nodes to request and maintain replay bundles: (a) initial bundle transfer, (b) incremental WAL streaming, (c) bundle compaction and retention policies.
- Phase 2 - Query Routing: Implement query router that: (a) analyzes query dependencies, (b) determines which edge nodes have sufficient coverage, (c) routes to nearest sufficient node or origin.
- Phase 3 - Consistency Levels: Define consistency levels based on bundle freshness: (a) 'strong' - routed to origin, (b) 'bounded staleness' - edge with recent bundle, (c) 'eventual' - any cached result.
- Phase 4 - Adaptive Sizing: Implement controller that adjusts bundle coverage per edge node based on: (a) query patterns, (b) network costs, (c) edge storage capacity.

## Evaluation Plan

- **E1:** Read latency reduction: edge-served vs. origin-served queries across geographic distances.
- **E2:** Staleness distribution: how stale are edge-served results under various update rates?
- **E3:** Bandwidth costs: bundle transfer vs. full replication vs. per-query origin access.
- **E4:** Query coverage: what fraction of queries can be served from edge at various bundle sizes?
- **E5:** Geo-distributed benchmark: YCSB across simulated edge locations.

## Expected Contributions

- Replay bundles as a partial replication primitive for edge computing.
- Query routing protocol based on WAL coverage analysis.
- Adaptive bundle sizing algorithm for edge deployments.
- Continuum framework connecting caching, partial replication, and full replication.

## Key Related Work

- Nishimura et al. 'MD-HBase' (2011); Tao et al. 'Facebook TAO' (2013)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.

Current adaptation:

- `maintenance.replay.export` now accepts optional primary-key redaction with `none`, `hash_pk`, and `drop_pk` modes.
- Redacted replay bundles carry optional `redaction` metadata and compute table, bundle, and performance checksums after redaction.
- `edge.bundle.request` continues to use the same redaction mode vocabulary for bounded change-window bundles.
