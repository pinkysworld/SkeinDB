# R06 — Forensic Query Language for Hash-Chained WAL

**Area:** Security & Privacy

## Problem Statement

SkeinDB's tamper-evident WAL enables forensic analysis, but the paper doesn't describe how to query it. Security investigations require answering temporal questions with cryptographic verification: 'Show all writes to table X between events Y and Z, and prove completeness.' A forensic query language built on the hash chain could provide both expressive queries and verifiable results.

## Research Hypotheses

- **H1:** A specialized query language for hash-chained logs can express common forensic questions more naturally than adapting SQL.
- **H2:** Cryptographic completeness proofs (proving no records were omitted) can be generated efficiently alongside query results.
- **H3:** Incremental verification (building on previously verified prefixes) can amortize proof checking costs for ongoing investigations.

## Methodology

- Phase 1 - Language Design: Design SkeinForensic, a query language supporting: (a) temporal predicates (BETWEEN timestamp, AFTER event), (b) entity tracking (all operations by user X), (c) causal queries (operations dependent on write W), (d) integrity assertions (PROVE COMPLETE).
- Phase 2 - Proof System: Develop proof generation for forensic queries. Each result includes: (a) Merkle proofs for included records, (b) boundary proofs showing query range coverage, (c) absence proofs for negation queries.
- Phase 3 - Index Structures: Design auxiliary indexes over the WAL that accelerate forensic queries while preserving verifiability. Indexes must be consistent with the hash chain.
- Phase 4 - Incremental Verification: Implement checkpoint-based verification where investigators can build on previously verified log prefixes, reducing redundant verification work.

## Evaluation Plan

- **E1:** Query expressiveness: can SkeinForensic express common investigation scenarios from security incident reports?
- **E2:** Proof generation time and proof size for various query types.
- **E3:** Verification time for proofs of varying complexity.
- **E4:** Index overhead (space, maintenance) vs. query acceleration.
- **E5:** Case study: simulate a data breach investigation using SkeinForensic.

## Runtime Status (2026-05-11)

R06 is now implemented as a hardened experimental runtime surface in v0.3.13:

- `forensic.query` accepts table/op/id bounds plus a SkeinForensic JSON filter grammar (`and`, `or`, `not`, comparison operators, and `contains`).
- Query proofs use `skein.forensic.proof.v1` with boundary hashes, checkpoint anchors, chain/Merkle roots, per-record inclusion proofs, and a chain-consistent index summary by time/id range, table, operation, and actor bucket.
- `forensic.verify` verifies contiguous returned record slices and detects tampering.
- `forensic.export` emits `skein.forensic.bundle.v1` report bundles with a query manifest, records, proof, and verification summary.
- SkeinAdmin's Forensics panel exposes chain health, query, proof verify, and export controls.
- Focused coverage includes RPC roundtrips, tamper detection, checkpoint-anchor proof metadata, and a simulated incident-timeline export harness.

Current limitations remain research-visible: the runtime records operation metadata rather than full WAL payload bytes, absence proofs for arbitrary negation queries are not yet implemented, and authenticated actor attribution is currently summarized as `unknown` until principal metadata is attached to forensic records.

## Expected Contributions

- First forensic query language designed for cryptographically verifiable logs.
- Proof system for completeness and integrity of forensic query results.
- Verifiable index structures for accelerating forensic queries.
- Practical framework for database-level security investigations.

## Key Related Work

- Crosby & Wallach 'Efficient Data Structures for Tamper-Evident Logging' (2009); Pulls & Dahlberg 'Transparency Logs' (2023)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.
