# R05 — Oblivious Query Execution for Multi-Tenant Deployments

**Area:** Security & Privacy

## Problem Statement

SkeinDB targets shared hosting and single-binary deployments where multiple tenants may share infrastructure. Even with proper access controls, access pattern leakage can reveal sensitive information across tenants. Traditional oblivious RAM techniques are expensive, but SkeinDB's architecture (ValueStore with content-addressed lookup, LSM organization) may enable more efficient oblivious execution strategies tailored to database workloads.

## Research Hypotheses

- **H1:** Database-specific access patterns (sequential scans, index lookups, range queries) can be protected more efficiently than generic ORAM by exploiting query structure.
- **H2:** The ValueStore's content-addressing already provides some obfuscation; targeted padding and shuffling can achieve formal obliviousness with acceptable overhead.
- **H3:** Tiered obliviousness (stronger guarantees for sensitive tables, weaker for public data) provides practical privacy-performance tradeoffs.

## Methodology

- Phase 1 - Threat Model: Formalize the multi-tenant threat model. Adversary observes: (a) I/O patterns to storage, (b) timing of operations, (c) memory access patterns (for side-channel attacks). Define what information leakage is acceptable.
- Phase 2 - Pattern Analysis: Characterize SkeinDB's access patterns for common query types. Identify which patterns leak information and which are inherently obfuscated by the architecture.
- Phase 3 - Oblivious Primitives: Design oblivious versions of key operations: (a) ValueStore lookup with padding and dummy accesses, (b) Index traversal with oblivious sorting, (c) Scan operations with deterministic padding.
- Phase 4 - Tiered System: Implement policy-based obliviousness levels. Administrators can mark tables/columns as requiring oblivious access, with the system automatically applying appropriate protections.

## Evaluation Plan

- **E1:** Overhead (latency, I/O, storage) of oblivious execution at various security levels.
- **E2:** Information leakage analysis using mutual information metrics.
- **E3:** Comparison with Path ORAM and other generic ORAM schemes.
- **E4:** Real-world attack simulation: can an adversarial tenant infer co-tenant query patterns?
- **E5:** Scalability: overhead as number of tenants increases.

## Expected Contributions

- Database-specific oblivious access patterns exploiting LSM and content-addressing structure.
- Formal analysis of information leakage in multi-tenant database deployments.
- Tiered obliviousness framework balancing security and performance.
- Practical implementation targeting shared hosting scenarios.

## Key Related Work

- Stefanov et al. 'Path ORAM' (2013); Crooks et al. 'Obladi' (2018); Eskandarian & Zaharia 'ObliDB' (2019)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
