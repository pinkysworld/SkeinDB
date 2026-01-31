# R20 — Energy-Aware Compaction Scheduling

**Area:** Systems Research

## Problem Statement

SkeinDB's workload-guided compaction considers performance metrics, but not energy consumption. For edge/embedded deployments, battery life is critical. For cloud deployments, energy costs and carbon footprint matter. An energy-aware compaction scheduler could defer work to periods of low activity, external power availability, or favorable electricity pricing, while maintaining acceptable performance bounds.

## Research Hypotheses

- **H1:** Compaction timing significantly impacts energy consumption due to SSD write amplification and CPU utilization patterns.
- **H2:** Deferring compaction to off-peak periods reduces energy costs without unacceptable performance degradation for typical workloads.
- **H3:** Energy-aware scheduling with performance constraints (max staleness, max read amplification) provides practical tradeoffs.

## Methodology

- Phase 1 - Energy Modeling: Build energy model for compaction: (a) measure energy per compaction operation, (b) model energy as function of LSM state and compaction size, (c) account for SSD garbage collection interactions.
- Phase 2 - Constraint Specification: Define performance constraints: (a) maximum read amplification, (b) maximum write amplification, (c) maximum compaction backlog (space). Constraints bound how much compaction can be deferred.
- Phase 3 - Scheduling Algorithm: Design scheduler that: (a) predicts future compaction needs, (b) estimates energy cost at different times, (c) schedules compaction to minimize energy while satisfying constraints.
- Phase 4 - External Signals: Integrate external signals: (a) power source (battery vs. plugged), (b) electricity pricing, (c) carbon intensity of grid, (d) predicted workload.

## Evaluation Plan

- **E1:** Energy consumption comparison: energy-aware vs. default compaction scheduling.
- **E2:** Performance impact: latency distribution with deferred compaction.
- **E3:** Constraint satisfaction: does scheduler maintain performance bounds?
- **E4:** Battery life extension for edge/embedded scenarios.
- **E5:** Cost reduction using time-of-use electricity pricing.

## Expected Contributions

- Energy model for LSM compaction operations.
- Constrained optimization framework for compaction scheduling.
- Integration of external signals (power source, pricing) into database scheduling.
- Empirical study of energy-performance tradeoffs in databases.

## Key Related Work

- Harizopoulos et al. 'Energy-Efficient Query Processing' (2008); Tsirogiannis et al. 'Analyzing the Energy Efficiency of DBMS' (2010)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
