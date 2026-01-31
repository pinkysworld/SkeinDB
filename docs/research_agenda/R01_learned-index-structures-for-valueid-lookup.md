# R01 — Learned Index Structures for ValueID Lookup

**Area:** Storage & Query Optimization

## Problem Statement

SkeinDB's ValueID-first encoding creates a unique opportunity for learned indexes. Traditional B-tree and hash indexes treat keys as opaque byte sequences, missing optimization opportunities when keys have exploitable structure. ValueIDs, being content-derived, exhibit distribution patterns that could be predicted by machine learning models, potentially replacing or augmenting traditional index structures with learned alternatives that offer O(1) lookup with minimal space overhead.

## Research Hypotheses

- **H1:** The content-derived nature of ValueIDs creates predictable distribution patterns that learned index models can exploit to achieve sub-logarithmic lookup times.
- **H2:** Hybrid indexes combining learned models with traditional fallback structures will achieve better space-time tradeoffs than pure B-trees for ValueStore workloads.
- **H3:** The deduplication property of ValueIDs (identical content maps to identical IDs) creates clustering effects that improve learned index accuracy compared to arbitrary key distributions.

## Methodology

- Phase 1 - Distribution Analysis: Instrument SkeinDB to collect ValueID distributions across diverse workloads (web applications, document stores, time-series). Analyze entropy, clustering, and temporal patterns.
- Phase 2 - Model Selection: Evaluate learned index architectures (RMI, PGM-index, ALEX) for ValueID lookup. Implement recursive model indexes with piece-wise linear functions as the base case.
- Phase 3 - Hybrid Design: Develop a hybrid structure where learned models handle the common case and B-tree segments handle outliers. Design online model updates during compaction.
- Phase 4 - Integration: Integrate learned indexes into SkeinDB's ValueStore, replacing or augmenting existing lookup structures. Implement graceful degradation when model accuracy drops.

## Evaluation Plan

- **E1:** Measure lookup latency (p50, p99, p99.9) compared to B-tree baseline across workload types.
- **E2:** Quantify space overhead of learned models vs. traditional indexes.
- **E3:** Evaluate model training time and its impact on compaction latency.
- **E4:** Test accuracy degradation under distribution shift and measure retraining frequency.
- **E5:** Benchmark on YCSB workloads with varying read/write ratios.

## Expected Contributions

- First application of learned indexes to content-addressed storage systems.
- Characterization of ValueID distributions and their learnability.
- Hybrid learned/traditional index design optimized for LSM compaction workflows.
- Open-source implementation integrated with SkeinDB.

## Key Related Work

- Kraska et al. 'The Case for Learned Index Structures' (2018); Ferragina & Vinciguerra 'PGM-index' (2020); Ding et al. 'ALEX' (2020)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
