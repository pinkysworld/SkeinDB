# R04 — Differentially Private Aggregate Queries via SkeinQL

**Area:** Security & Privacy

## Problem Statement

Organizations increasingly need to query sensitive data while protecting individual privacy. Differential privacy provides formal guarantees but is difficult to retrofit onto SQL. SkeinQL's structured, versioned interface creates an opportunity to enforce differential privacy at the API level, with privacy budget management integrated into the query lifecycle. The ETag system could signal when cached results remain within privacy budgets.

## Research Hypotheses

- **H1:** SkeinQL's structured query representation enables automated sensitivity analysis for common aggregate queries without requiring user-provided sensitivity bounds.
- **H2:** Privacy budget integration with query fingerprints allows efficient budget tracking and enforcement across sessions.
- **H3:** ETag-based caching can be extended to privacy-aware caching where cached results are reused only when they satisfy both freshness and privacy constraints.

## Methodology

- Phase 1 - Sensitivity Analysis: Implement automatic sensitivity computation for SkeinQL aggregate operations (COUNT, SUM, AVG, percentiles). Handle joins and group-by through composition theorems.
- Phase 2 - Budget Management: Design a privacy budget manager that: (a) associates budgets with users/roles, (b) tracks consumption per query fingerprint, (c) supports budget refresh policies (daily, per-session).
- Phase 3 - Noise Mechanisms: Integrate calibrated noise addition (Laplace for numeric, exponential mechanism for categorical) into query execution. Support both global and local DP models.
- Phase 4 - Cache Integration: Extend ETag semantics to include privacy metadata. A cached result's ETag encodes both data freshness and privacy cost, enabling privacy-aware cache validation.

## Runtime status (v0.3.11)

The first hardened SkeinDB slice now implements `dp.aggregate` for COUNT/SUM/AVG with explicit bounds and DP parameters, persisted per-principal budgets, seeded Laplace/Gaussian mechanisms, budget-consumption audit events, and `privacy_etag` validators derived from DP metadata plus table versions. The evaluation harness lives in `dp.evaluate`.

## Evaluation Plan

- **E1:** Accuracy of sensitivity analysis compared to ground truth on benchmark queries.
- **E2:** Utility loss (query accuracy) at various privacy levels (epsilon = 0.1, 1, 10).
- **E3:** Budget consumption patterns under realistic analyst workloads.
- **E4:** Performance overhead of privacy enforcement.
- **E5:** Comparison with PINQ and Google's DP SQL on standard benchmarks.

## Expected Contributions

- First integration of differential privacy into a database's native API layer (vs. external query rewriting).
- Automatic sensitivity analysis for structured query operations.
- Privacy-aware cache validation protocol extending HTTP ETags.
- Practical budget management system for multi-user environments.

## Key Related Work

- McSherry 'Privacy Integrated Queries' (2009); Wilson et al. 'Differentially Private SQL' (2020); Kotsogiannis et al. 'Privates' (2019)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
