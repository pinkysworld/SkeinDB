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
- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
