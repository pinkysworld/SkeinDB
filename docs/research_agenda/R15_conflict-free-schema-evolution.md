# R15 — Conflict-Free Schema Evolution

**Area:** Consistency & Distribution

## Problem Statement

Schema changes are notoriously difficult in distributed databases, often requiring coordinated downtime or complex migration procedures. SkeinDB's MVCC and dependency tracking could support concurrent schema evolution, where different nodes temporarily operate with different schema versions. The system would automatically reconcile when versions merge, similar to how Git handles concurrent file changes.

## Research Hypotheses

- **H1:** Many schema changes (adding columns, adding indexes, renaming) can be applied concurrently without conflicts.
- **H2:** MVCC version metadata can track schema version alongside data version, enabling queries to be evaluated against the appropriate schema.
- **H3:** Schema conflicts (incompatible column type changes, constraint additions) can be detected and reported for manual resolution.

## Methodology

- Phase 1 - Schema Versioning: Extend MVCC metadata to include schema version. Each row version is tagged with the schema version it was written under.
- Phase 2 - Concurrent Evolution: Implement protocol for concurrent schema changes: (a) nodes propose changes, (b) non-conflicting changes apply independently, (c) conflicting changes queue for resolution.
- Phase 3 - Query Adaptation: Implement query execution that handles schema heterogeneity: (a) detect schema mismatch between query and data, (b) apply automatic conversion where possible, (c) fail gracefully for incompatible schemas.
- Phase 4 - Merge Protocol: Design schema merge protocol: (a) detect when nodes have diverged, (b) compute merged schema, (c) propagate merge to all nodes.

## Evaluation Plan

- **E1:** Schema evolution latency: concurrent changes vs. coordinated changes.
- **E2:** Conflict rate: what fraction of real-world schema changes conflict?
- **E3:** Query performance during schema divergence.
- **E4:** Merge correctness: do merged schemas preserve application semantics?
- **E5:** Case study: simulate rolling deployment with schema changes across data centers.

## Expected Contributions

- First conflict-free schema evolution protocol for distributed databases.
- MVCC extension for schema versioning alongside data versioning.
- Automatic schema conversion during query execution.
- Analysis of schema change compatibility in real-world applications.

## Key Related Work

- Curino et al. 'Schema Evolution in Wikipedia' (2008); Kleppmann 'Schema Evolution in Avro, Protocol Buffers, and Thrift' (2017)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Prototype status (2026-05-15):** MVCC row entries now persist `schema_version` alongside row/data version metadata in table `.json` / `.rseg` payloads, `schema.apply_merge` stamps rewritten rows with the applied schema version, and legacy v2 row payloads normalize from `schema_versions.json` on load. Focused evidence lives in `engine::tests::schema_version_tags_row_entries_and_normalizes_legacy_rows`.
- **Concurrent merge status (2026-05-15):** `schema.propose_change` now accepts `add_index` in addition to `add_column`, `schema.merge_status` simulates earlier pending changes so non-conflicting column/index proposals merge in-order, duplicate index definitions surface deterministic `index_conflict:*` reasons, and the persisted schema-change log is now versioned as `schema_changes.json` format v2. Focused evidence lives in `engine::tests::schema_evolution_merges_concurrent_column_and_index_changes` and `cluster_rpc.rs::r15_schema_evolution_concurrent_column_and_index_changes`.
- **Query adaptation status (2026-05-16):** `query.select` and adjacent executor paths now materialize heterogeneous legacy rows through MySQL-compatible column defaults, falling back to `NULL` across batch/non-batch scans, keyed reads, joins, and interned predicate evaluation so current-schema reads no longer fail with spurious missing-column errors. Focused evidence lives in `engine::tests::query_select_adapts_legacy_rows_to_schema_defaults`.
- **Merge protocol status (2026-05-16):** `schema.apply_merge` now deterministically rolls forward eligible merge-plan winners while marking terminal losers `rejected`, returns `rolled_back` conflict details to RPC callers, and removes resolved losers from later `schema.merge_status` output so merge completion collapses divergence instead of leaving loser proposals pending. Focused evidence lives in `engine::tests::schema_evolution_merges_concurrent_column_and_index_changes` and `cluster_rpc.rs::r15_schema_evolution_concurrent_column_and_index_changes`.
- **Migration assistant status (2026-05-16):** `schema.merge_status` now returns a structured `resolution` plan that classifies each pending proposal as `roll_forward`, `rollback`, or `wait`, pairing the raw conflict reason with a caller-facing suggestion so divergence can be reviewed before a merge is applied. Focused evidence lives in `engine::tests::schema_merge_status_proposes_resolution_actions` and `cluster_rpc.rs::r15_schema_evolution_concurrent_column_and_index_changes`.
- **Rolling-deploy harness status (2026-05-16):** `schema.simulate_rollout` now projects prepare/mixed/steady-state deployment waves across configurable node counts, reuses `schema.merge_status` guidance as preflight advice, and reports when legacy rows will still require query-time schema adaptation after the merged version lands. Focused evidence lives in `engine::tests::schema_simulate_rollout_reports_mixed_version_waves` and `cluster_rpc.rs::r15_schema_evolution_concurrent_column_and_index_changes`.
- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
