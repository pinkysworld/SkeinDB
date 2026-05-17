# R02 — Adaptive Row-Column Hybrid Execution

**Area:** Storage & Query Optimization

## Problem Statement

SkeinDB mentions 'optional column snapshots for analytics' but doesn't formalize when to materialize them or how they interact with the dependency tracking system. Modern workloads increasingly mix OLTP and OLAP patterns. A principled approach to automatic column projection materialization could provide HTAP capabilities without the complexity of separate engines, using SkeinDB's existing infrastructure for cache invalidation.

## Research Hypotheses

- **H1:** Query pattern analysis can predict which column projections will yield the highest benefit-to-cost ratio for materialization.
- **H2:** SkeinDB's dependency tracking can be extended to maintain column snapshot consistency with minimal overhead compared to full table scans.
- **H3:** Adaptive materialization decisions can be made online with acceptable overhead, avoiding the need for offline workload analysis.

## Methodology

- Phase 1 - Cost Model Development: Formalize the cost of column snapshot creation (scan cost, storage overhead) versus benefit (reduced I/O for projection queries). Model includes compaction interaction costs.
- Phase 2 - Pattern Detection: Implement query pattern analysis that identifies: (a) frequently accessed column subsets, (b) scan-heavy queries that would benefit from columnar storage, (c) temporal patterns in column access.
- Phase 3 - Dependency Integration: Extend dependency tracking to column granularity. When a row is updated, mark affected column snapshots for incremental refresh or invalidation.
- Phase 4 - Adaptive Materialization: Implement a controller that continuously evaluates materialization decisions based on recent query patterns and resource availability.

## Evaluation Plan

- **E1:** CH-benCHmark (combined OLTP/OLAP) performance vs. row-only and full-columnar baselines.
- **E2:** Measure adaptation latency when workload shifts from OLTP-heavy to analytics-heavy.
- **E3:** Storage overhead of maintaining column snapshots under various update rates.
- **E4:** Compare with commercial HTAP systems (TiDB, SingleStore) on mixed workloads.

## Expected Contributions

- Formal cost model for column snapshot materialization in LSM-based systems.
- Integration of columnar storage with fine-grained dependency tracking.
- Online adaptive algorithm for materialization decisions.
- Empirical study of row-column tradeoffs in modern workloads.

## Key Related Work

- Arulraj et al. 'Bridging the Archipelago between Row-Stores and Column-Stores' (2016); Pavlo et al. 'Self-Driving Database Management Systems' (2017)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

Current prototype status:
- Phase 1's build-vs-benefit pricing is implemented for single-table SELECTs.
- Candidate snapshot build cost is evaluated against the live table row count so selective probes do not underestimate full materialization cost.
- Phase 2's hot projection detector is implemented as a bounded per-table set of normalized column patterns ranked by frequency, scan volume, and recency.
- Phase 3's dependency-driven maintenance preserves unaffected snapshots across schema-version changes and invalidates only snapshots/patterns that depend on dropped columns.
- Phase 4's online controller can replace a broader active covering snapshot with a narrower hot projection when the additional build cost is repaid by lower snapshot scan cost.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
