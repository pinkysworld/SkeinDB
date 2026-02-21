# SkeinDB: A Single-Binary Database with SkeinQL, Cluster Control-Plane RPC, and a Web-Native Administration Stack

**Manuscript type:** Original research article (systems/database engineering)  
**Target venue:** International Journal of Research in Computing (IJRC / IJRCOM)  
**Version:** Camera-ready draft v2 (2026-02-21)

## Author and manuscript metadata

### Title
SkeinDB: A Single-Binary Database with SkeinQL, Cluster Control-Plane RPC, and a Web-Native Administration Stack

### Authors
- Michel Picker (Corresponding Author)
- _Add co-authors as needed_

### Affiliations
- _Add institution/company and country for each author_

### Correspondence
- _Add full postal address and email_

### ORCID
- _Add ORCID for each author_

### CRediT contributions (to finalize)
- Conceptualization: Michel Picker  
- Methodology: Michel Picker  
- Software: Michel Picker  
- Validation: Michel Picker  
- Writing – original draft: Michel Picker  
- Writing – review & editing: _Add contributors_

## Abstract

**Background/Context (50-60 words).** Database engineering teams often face a tradeoff between deployability and research velocity: production systems are operationally robust but hard to extend experimentally, while research systems are novel but difficult to operate. SkeinDB addresses this gap by packaging compatibility paths, web-native APIs, and experimental database features into one executable process with a unified control plane.

**Objective/Aim (40-50 words).** This work presents the design and implementation of SkeinDB with emphasis on: (i) a persistent cluster control-plane with `cluster.*` RPC methods, (ii) primary-to-replica replication transport in prototype form, and (iii) a phpMyAdmin-like web administration experience that exposes operational and research features through a single interface.

**Methods/Approach (60-70 words).** We implemented typed SkeinQL method families, durable cluster metadata (`cluster.state.v1`), shard placement operations, and role-aware write guards. We integrated these capabilities into an embedded web UI supporting connect/disconnect, cluster workflows, settings management, and full RPC exploration. Evaluation combines unit tests, integration tests (including multi-node cluster replication), QUIC transport tests, and reproducible build-and-test runs.

**Results/Findings (70-80 words).** The current prototype exposes 74 RPC methods, including 9 cluster control-plane methods (`cluster.status`, node join/leave, promotion, shard create/move/rebalance). The automated suite executes 111 tests across crates and transports with all tests passing; full workspace `cargo test` completes in 22.79 seconds on the evaluated ARM64 macOS environment. Cluster integration tests validate schema+data replication from primary to replica via RPC fanout; QUIC tests validate protocol roundtrips and 0-RTT write rejection behavior.

**Implications/Conclusions (40-50 words).** SkeinDB demonstrates that a single-binary architecture can provide practical operator workflows while preserving rapid systems-research extensibility. The implemented control-plane and admin stack are production-shape interfaces with prototype internals. Future work targets WAL/LSN + CAS replication, fault-aware routing, and deeper benchmark validation to transition from research prototype to production-grade distributed operation.

## Keywords
single-binary database; JSON-RPC database API; SkeinQL; cluster control-plane; web-native administration

## 1. Introduction

The dominant pattern in data systems separates operational databases from research experimentation. Operational systems optimize reliability and ecosystem compatibility, while research systems optimize novelty and iteration speed. In practice, this separation creates friction: teams either delay innovation due to operational constraints or accept brittle prototypes that are costly to operationalize.

SkeinDB is designed to reduce this gap through three core decisions:

1. **Single executable deployment.** One process exposes HTTP RPC, embedded administration UI, optional QUIC RPC, and storage/engine primitives.
2. **Explicit typed control plane.** SkeinQL JSON-RPC methods define behavior as stable API contracts rather than implicit side effects.
3. **Research-first extensibility.** Experimental capabilities are integrated as method families with tests and docs, not separate throwaway branches.

This paper focuses on the latest implementation milestone: a usable cluster control-plane with persistent state, shard operations, and replication transport; plus a phpMyAdmin-like administration surface that makes these controls accessible without CLI-only workflows.

### 1.1 Contributions

This manuscript makes five concrete contributions:

- **C1: Cluster control-plane implementation.** A `cluster.*` method family with persistent metadata and role-aware operations.
- **C2: Prototype replication transport.** Primary-to-replica RPC fanout with loop prevention and replication counters.
- **C3: Integrated administration UX.** Embedded SkeinAdmin with dedicated admin and console routes, profile-driven connectivity, and cluster/settings workflows.
- **C4: Verified method surface expansion.** 74 methods exposed through capabilities, including 9 cluster methods.
- **C5: Reproducible validation baseline.** End-to-end automated tests (111 total) covering unit, integration, transport, and cluster scenarios.

## 2. Related Work and Positioning

SkeinDB intersects prior themes in databases and systems:

- **Learned structures and adaptive storage:** model-guided indexing and hybrid layouts motivate SkeinDB research tracks for ValueID lookup and row/column adaptivity [1], [2].
- **Web and sandbox execution:** WebAssembly informs safe operator extension paths and constrained compute deployment [3].
- **Consistency and replication:** causal/session semantics and distributed state management motivate ETag-chain consistency and cluster plans [4].
- **Privacy and secure query processing:** differential privacy and oblivious techniques are represented as dedicated SkeinQL surfaces [5], [6].
- **Materialized and incremental maintenance:** dependency-aware refresh strategies inspire SkeinDB incremental view tracks [7].

Unlike systems that treat administration as an external control plane, SkeinDB intentionally co-locates operator UX and method contracts inside one binary to maximize local reproducibility and reduce configuration overhead.

## 3. System Overview

### 3.1 Process architecture

A SkeinDB process hosts:

- HTTP SkeinQL endpoint (`/api/v1/rpc`)
- embedded admin UI routes (`/admin`, `/console`)
- optional QUIC endpoint
- execution engine and settings persistence

This shape enables local and edge deployments with minimal orchestration while preserving API richness.

### 3.2 API model

SkeinQL methods are versioned and typed. Method families include:

- core: `system.*`, `transport.*`, `stats.*`, `settings.*`
- schema/data/query: `schema.*`, `data.*`, `query.*`
- cluster: `cluster.*`
- experimental: `vector.*`, `dp.*`, `oblivious.*`, `forensic.*`, `migration.*`, `wasm.plan.*`, and related families

At runtime, `system.capabilities` surfaces enabled methods and feature flags for client introspection.

### 3.3 Persistence model

Operational metadata is persisted in settings storage. Cluster metadata is serialized under `cluster.state.v1`, including node list, roles, join tokens, shard ownership, and replication counters. This avoids dependence on an external metadata service at prototype stage while keeping state durable across restarts.

## 4. Cluster Control-Plane Design and Implementation

### 4.1 Implemented RPC methods

The cluster control-plane currently exposes:

1. `cluster.status`
2. `cluster.nodes`
3. `cluster.join_token.create`
4. `cluster.node.join`
5. `cluster.node.remove`
6. `cluster.replica.promote`
7. `cluster.shard.create`
8. `cluster.shard.move`
9. `cluster.shard.rebalance`

These methods are typed in the SkeinQL schema crate and dispatched in server RPC handling.

### 4.2 Node identity and lifecycle

Each node has a local identity and role metadata. Join token issuance is explicit and time-bounded. Joining a node updates persisted cluster metadata and can assign replica/primary role semantics.

### 4.3 Write ownership and routing guard

When clustering is enabled, write methods are validated against cluster primary ownership. If the local node is not the shard/global primary for the target object, writes are rejected with a structured error. This enforces consistent authority boundaries before advanced router failover automation is introduced.

### 4.4 Shard metadata and placement

Shard records map `(db, table)` scopes to primary + replica node assignments. Operators can create shard ownership, move ownership, and request rebalance plans. In prototype stage, rebalance is policy-light but operationally explicit.

### 4.5 Replication transport (prototype)

Successful primary writes are faned out to replica nodes over HTTP SkeinQL RPC. Replicated requests carry an internal header (`x-skeindb-replication`) to prevent recursive propagation loops. Replication counters include shipped operations, failed operations, and last-error metadata.

This is intentionally a transitional transport model. It optimizes implementation clarity and testability over maximum throughput.

## 5. SkeinAdmin UX: phpMyAdmin-like but RPC-native

### 5.1 Route and mode model

SkeinAdmin is served in two operator modes:

- `/admin`: full control-plane mode
- `/console`: SQL/workspace-first mode

Both modes share one UI bundle but expose different default navigation emphases.

### 5.2 Panel architecture

The embedded UI provides 9 primary panels:

1. overview
2. workspace (SQL)
3. schema
4. data
5. cluster
6. settings
7. migration
8. natural-language lab
9. RPC explorer

### 5.3 Operator affordances

Recent UX additions include:

- explicit **Connect/Disconnect** controls with status badges
- connection profiles for multi-target workflows
- cluster operation controls (token create, join, remove, promote, shard operations)
- settings management through `settings.get`/`settings.set`
- capability-driven method discovery through RPC explorer

The design goal is “all features controllable from web UI,” with RPC Explorer serving as a safe universal fallback for newly added methods.

## 6. Methods and Evaluation Protocol

### 6.1 Evaluation focus

Given the prototype maturity stage, evaluation priorities are:

- **correctness of control-plane transitions**
- **protocol roundtrip reliability**
- **transport interoperability (HTTP + QUIC)**
- **regression resistance through automated tests**

### 6.2 Test structure

Validation uses:

- unit tests for engine and server behavior
- multi-process integration tests for cluster replication
- QUIC integration tests for transport correctness and safety behavior
- workspace-level build/test tooling (`cargo fmt`, `cargo clippy`, `cargo test`)

### 6.3 Reproducibility environment

The reported run used:

- OS: Darwin 25.3.0 (ARM64)
- Rust: `rustc 1.92.0`
- Cargo: `cargo 1.92.0`

## 7. Results

### 7.1 Feature surface results

**R1 — Method coverage.** The runtime capability endpoint reports **74 methods**. Of these, **9 methods** are cluster control-plane methods.

**R2 — Admin control coverage.** Cluster and settings operations are directly invokable from SkeinAdmin panels, with RPC Explorer exposing complete method-level fallback.

### 7.2 Automated validation results

**R3 — Total tests.** The workspace currently executes **111 tests** across crates and integration suites.

**R4 — Full test runtime.** Full `cargo test` completed in **22.79 seconds** in the reference environment.

**R5 — Cluster replication integration.** The dedicated cluster integration test validated schema and row replication with pass runtime of approximately **3.05 seconds**.

**R6 — QUIC transport integration.** QUIC test suite ran **13 tests**, all passing, with suite runtime approximately **15.70 seconds**, including zero-RTT write safety behavior.

### 7.3 Summary table

| Metric | Value | Evidence source |
|---|---:|---|
| Total SkeinQL methods | 74 | `system.capabilities` runtime query |
| Cluster control-plane methods | 9 | `system.capabilities` method list |
| Total automated tests | 111 | `cargo test -- --list` aggregate |
| Full test runtime | 22.79 s | `/usr/bin/time -p cargo test` |
| Cluster integration runtime | 3.05 s | `tests/cluster_rpc.rs` run |
| QUIC integration runtime | 15.70 s | `tests/quic_rpc.rs` run |

## 8. Discussion

### 8.1 What this validates

The results support three conclusions:

1. **Control-plane completeness at prototype level.** Cluster lifecycle and shard operations are fully represented in the API and reachable via UI.
2. **Cross-surface coherence.** API, tests, and administration panel now expose the same cluster behaviors.
3. **Fast local verification cycle.** The full test cycle remains short enough for frequent iterative development.

### 8.2 Why this matters

Many research database prototypes fail to bridge from demonstration to operator usability. SkeinDB’s approach is to mature interfaces first: stable method contracts, durable metadata semantics, and an operator-facing UI that encourages early operational feedback.

### 8.3 Current limitations

This implementation remains **prototype-grade** in several areas:

- replication uses RPC fanout, not WAL/LSN streaming
- no external consensus/governance for primary election
- shard rebalance policy is intentionally simple
- performance evidence currently emphasizes correctness and reproducibility over throughput/latency benchmarks

## 9. Threats to Validity

- **Single-environment measurements.** Timings reported from one hardware/software environment may not generalize.
- **Correctness-heavy evaluation.** Passing tests establish functional behavior but not production-scale failure resilience.
- **Prototype transport model.** RPC fanout is not representative of mature replication throughput or durability semantics.
- **Feature maturity variance.** “Implemented” across research tracks often means validated prototype surfaces rather than finalized production-grade subsystems.

## 10. Roadmap to Production-Grade Cluster Operation

Near-term work is organized around four upgrades:

1. **WAL/LSN-native replication path** with idempotent apply semantics.
2. **CAS object-manifest pull** for efficient distributed state movement.
3. **Router/failover automation** for read balancing and role transition.
4. **Benchmark program expansion** (load, failover, long-run stability, and storage efficiency).

These upgrades preserve the current RPC contracts while deepening transport and runtime guarantees.

## 11. Conclusion

SkeinDB demonstrates a practical path toward unifying operator ergonomics and systems research iteration in one deployable binary. The cluster control-plane and administration stack now provide an end-to-end management substrate: persistent metadata, node lifecycle controls, shard operations, replication fanout, and web-based execution surfaces.

The current artifact should be interpreted as a high-fidelity prototype: interface-complete for core operations, well-tested for correctness, and ready for the next phase of replication, routing, and benchmark hardening.

## References (IEEE style)

[1] T. Kraska, A. Beutel, E. H. Chi, J. Dean, and N. Polyzotis, "The Case for Learned Index Structures," in *Proc. ACM SIGMOD*, 2018.

[2] T. Neumann, "Efficiently Compiling Efficient Query Plans for Modern Hardware," *PVLDB*, vol. 4, no. 9, pp. 539-550, 2011.

[3] A. Haas et al., "Bringing the Web up to Speed with WebAssembly," in *Proc. PLDI*, 2017.

[4] W. Lloyd, M. Freedman, M. Kaminsky, and D. Andersen, "Don't Settle for Eventual: Scalable Causal Consistency for Wide-Area Storage with COPS," in *Proc. SOSP*, 2011.

[5] F. McSherry, "Privacy Integrated Queries: An Extensible Platform for Privacy-Preserving Data Analysis," in *Proc. ACM SIGMOD*, 2009.

[6] E. Stefanov et al., "Path ORAM: An Extremely Simple Oblivious RAM Protocol," in *Proc. ACM CCS*, 2013.

[7] A. Gupta and I. S. Mumick, "Maintenance of Materialized Views: Problems, Techniques, and Applications," *IEEE Data Eng. Bull.*, vol. 18, no. 2, pp. 3-18, 1995.

[8] S. Chaudhuri and V. Narasayya, "An Efficient Cost-Driven Index Selection Tool for Microsoft SQL Server," in *Proc. VLDB*, 1997.

[9] D. R. Karger et al., "Consistent Hashing and Random Trees: Distributed Caching Protocols for Relieving Hot Spots on the World Wide Web," in *Proc. STOC*, 1997.

[10] M. Stonebraker and U. Cetintemel, "One Size Fits All: An Idea Whose Time Has Come and Gone," in *Proc. ICDE*, 2005.

## Data and code availability

All source code, tests, and documentation artifacts are available in the project repository:  
[https://github.com/pinkysworld/SkeinDB](https://github.com/pinkysworld/SkeinDB)

The camera-ready draft source is stored in:
- `docs/papers/SkeinDB_IJRCOM_Submission.md`
- `docs/papers/SkeinDB_IJRCOM_Submission.docx`

## Funding statement

No external funding was received for this study.

## Conflicts of interest

The authors declare no conflicts of interest.

## Ethical considerations

This study does not involve human participants or animals.

## AI usage statement

Generative AI tools were used to support drafting, restructuring, and language refinement of manuscript text. All technical claims, implementation descriptions, and validation outcomes were reviewed and verified against repository artifacts and executed test outputs by the authors.

## Camera-ready submission checklist (author action items)

- Fill final author metadata table (names, affiliations, ORCID, correspondence).  
- Add final CRediT percentage table per IJRC template.  
- Paste this text into the IJRC Word template structure and preserve heading levels.  
- Verify all references include DOI/URL where available.  
- Insert any final figures/tables required by reviewers.  
- Export final PDF with embedded fonts and run one final proofread pass.
