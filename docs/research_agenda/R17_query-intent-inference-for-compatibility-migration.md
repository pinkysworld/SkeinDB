# R17 — Query Intent Inference for Compatibility Migration

**Area:** Developer Experience & Tooling

## Problem Statement

SkeinDB's compatibility telemetry identifies unsupported MySQL features, but knowing that a feature is unsupported doesn't help developers migrate. By analyzing query patterns and runtime behavior, SkeinDB could infer the intent behind queries and suggest SkeinQL equivalents that preserve intent rather than just syntax. For example, detecting that a series of queries implements pagination and suggesting a native cursor API.

## Research Hypotheses

- **H1:** Common application patterns (pagination, hierarchical queries, soft deletes) have recognizable query signatures that can be detected statically and dynamically.
- **H2:** Intent-preserving migrations to SkeinQL achieve better performance than syntax-preserving rewrites.
- **H3:** Developers accept and adopt intent-based migration suggestions at higher rates than syntactic suggestions.

## Methodology

- Phase 1 - Pattern Library: Build a library of common query patterns with their intents: (a) LIMIT/OFFSET pagination, (b) recursive CTEs for hierarchies, (c) COALESCE chains for defaults, (d) EXISTS subqueries for membership.
- Phase 2 - Pattern Detection: Implement pattern matching on incoming queries: (a) syntactic matching for single queries, (b) sequence matching for multi-query patterns, (c) dynamic analysis for parameter correlations.
- Phase 3 - Intent Mapping: For each detected pattern, map to SkeinQL intent-preserving alternative: (a) pagination to cursor API, (b) hierarchies to graph queries, (c) polling to CDC subscriptions.
- Phase 4 - Migration Assistant: Build migration assistant that: (a) presents detected patterns and suggested migrations, (b) provides before/after comparison, (c) offers automatic rewrite for safe transformations.

## Evaluation Plan

- **E1:** Pattern detection accuracy on open-source application codebases.
- **E2:** Performance comparison: original vs. migrated queries.
- **E3:** Developer study: comprehension and acceptance of migration suggestions.
- **E4:** Migration coverage: what fraction of queries can be automatically migrated?
- **E5:** Case study: migrate a real application (e.g., WordPress) from MySQL to SkeinDB.

## Expected Contributions

- Query intent inference from syntactic and dynamic analysis.
- Pattern library mapping MySQL idioms to SkeinQL features.
- Intent-preserving migration framework.
- Empirical study of automatic database migration tools.

## Key Related Work

- Cheung et al. 'Optimizing Database-Backed Applications' (2013); Barowy et al. 'CUSTODIAN' (2016)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
