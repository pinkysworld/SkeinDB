# R03 — Delta-Chain Topology Optimization

**Area:** Storage & Query Optimization

## Problem Statement

SkeinDB proposes delta-chained MVCC but doesn't specify the optimal chain structure. Linear delta chains degrade read performance as chain length grows. Alternative topologies (tree-structured deltas, skip-list shortcuts) could provide better read-write tradeoffs. The interaction between chain topology and LSM compaction creates a rich optimization space that hasn't been formally studied.

## Research Hypotheses

- **H1:** Skip-list-style delta chains (with periodic full snapshots) provide O(log n) version reconstruction while maintaining O(1) write amplification for updates.
- **H2:** Workload-aware chain topology (adapting structure based on version access patterns) outperforms static topologies.
- **H3:** Compaction can be leveraged to restructure delta chains opportunistically, amortizing reorganization costs.

## Methodology

- Phase 1 - Formal Model: Develop a cost model for delta chain operations: (a) version reconstruction cost as a function of chain length and topology, (b) write cost for appending deltas, (c) space overhead for different structures.
- Phase 2 - Topology Design: Implement and analyze three topologies: (a) Linear chains with periodic snapshots, (b) Skip-list chains with geometric snapshot spacing, (c) Tree-structured chains with branching at version forks.
- Phase 3 - Compaction Integration: Design compaction policies that consider delta chain state. During compaction, evaluate whether to: (a) consolidate deltas into full values, (b) restructure chain topology, (c) add skip pointers.
- Phase 4 - Adaptive Selection: Implement a controller that selects topology per-key based on observed access patterns (frequent historical reads vs. primarily latest-version access).

## Evaluation Plan

- **E1:** Version reconstruction latency as a function of version depth for each topology.
- **E2:** Write amplification under update-heavy workloads.
- **E3:** Space efficiency (delta compression ratio) across topologies.
- **E4:** Compaction overhead with topology-aware vs. topology-agnostic policies.
- **E5:** Time-travel query performance for various historical depths.

## Expected Contributions

- First formal analysis of delta chain topologies in LSM-based MVCC systems.
- Provable bounds on version reconstruction cost for skip-list delta chains.
- Compaction-integrated topology optimization algorithms.
- Empirical characterization of topology-workload interactions.

## Key Related Work

- Wu et al. 'An Empirical Evaluation of In-Memory MVCC' (2017); Neumann et al. 'Fast Serializable MVCC' (2015)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
