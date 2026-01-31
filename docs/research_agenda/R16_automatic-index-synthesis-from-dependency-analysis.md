# R16 — Automatic Index Synthesis from Dependency Analysis

**Area:** Developer Experience & Tooling

## Problem Statement

SkeinDB's dependency tracking knows which key ranges and indexes queries touch. This information, collected at runtime, captures actual access patterns more accurately than static analysis of EXPLAIN plans. By inverting the dependency relationship - from 'what does this query depend on' to 'what indexes would benefit this query' - SkeinDB could automatically synthesize optimal indexes.

## Research Hypotheses

- **H1:** Runtime dependency tracking captures access patterns that static analysis misses (e.g., correlated subqueries, dynamic predicates).
- **H2:** Index synthesis based on observed dependencies achieves better query performance than rule-based index advisors.
- **H3:** Continuous index adaptation (adding/removing indexes based on evolving patterns) maintains efficiency as workloads change.

## Methodology

- Phase 1 - Dependency Collection: Extend dependency tracking to record: (a) columns in predicates, (b) key range patterns, (c) join conditions, (d) ordering requirements. Aggregate across queries.
- Phase 2 - Candidate Generation: From aggregated dependencies, generate index candidates: (a) single-column indexes for equality predicates, (b) composite indexes for multi-column predicates, (c) covering indexes for frequently accessed column sets.
- Phase 3 - Cost-Benefit Analysis: For each candidate, estimate: (a) query speedup based on access pattern frequency, (b) write overhead for index maintenance, (c) storage cost. Select indexes with positive net benefit.
- Phase 4 - Online Adaptation: Implement continuous adaptation: (a) monitor dependency patterns, (b) propose new indexes, (c) retire unused indexes, (d) handle schema changes.

## Evaluation Plan

- **E1:** Query performance improvement vs. no indexes and vs. DBA-designed indexes.
- **E2:** Index overhead (storage, write latency) under various workloads.
- **E3:** Adaptation speed: how quickly does the system converge to good indexes after workload shift?
- **E4:** Comparison with commercial index advisors (SQL Server, PostgreSQL).
- **E5:** Long-running benchmark: index evolution over weeks of changing workloads.

## Expected Contributions

- Runtime dependency tracking for index synthesis.
- Cost-benefit model for continuous index adaptation.
- Online index evolution algorithm.
- Empirical study of automatic vs. manual index design.

## Key Related Work

- Chaudhuri & Narasayya 'AutoAdmin Index Advisor' (1998); Petraki et al. 'Automatic Index Management' (2015); Ding et al. 'AI Meets DB' (2019)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
