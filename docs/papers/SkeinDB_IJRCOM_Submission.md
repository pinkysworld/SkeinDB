# SkeinDB: A Single-Binary Web-Native Database with SkeinQL, Cluster Control-Plane, and Research-Driven Extensions

**Manuscript type:** Original research article (systems/database engineering)

**Target venue format:** IJRCOM-ready draft (structured manuscript)

**Proposed running title:** SkeinDB: Single-Binary DB with Web-Native Consistency and Cluster RPC

**Authors:**
- Michel Picker (Corresponding Author)
- _Add co-authors as needed_

**Affiliations:**
- _Add institution/company and country_

**Correspondence:**
- _Add full postal address and email_

## Abstract
SkeinDB is a single-executable database system designed to unify deployment simplicity, MySQL adoption paths, and research-oriented extensibility. The system exposes a native JSON-RPC interface (SkeinQL), a phpMyAdmin-like web control plane (SkeinAdmin), and a modular execution engine that supports advanced features including ETag-based cache-coherent queries, delta-style query patches, vector operations, differential privacy aggregates, forensic verification APIs, merge conflict handling, and WebAssembly-based query artifacts. In this work, we present the end-to-end architecture, API surface, and implementation strategy for SkeinDB, with special emphasis on the newly integrated cluster control-plane and replication transport. The implemented cluster subsystem provides persistent cluster state, node join/leave using short-lived tokens, replica promotion, shard creation/move/rebalance, and primary-to-replica write fanout over SkeinQL RPC. We evaluate system behavior through unit and integration test suites spanning HTTP and QUIC transports. Results demonstrate that SkeinDB can deliver a coherent single-process developer experience while serving as a practical platform for rapid systems research prototyping. We discuss tradeoffs, limitations, and the roadmap from prototype transport fanout to full WAL/CAS replication with routing and fault-tolerant automation.

**Keywords:** Single-binary database, JSON-RPC database API, SkeinQL, ETag consistency, cluster control-plane, sharding, replication transport, WebAssembly query execution, database research prototype

## 1. Introduction
Modern application teams frequently face a deployment and evolution mismatch: production databases prioritize operational maturity, while experimental capabilities (web-native consistency, AI-assisted query workflows, programmable operators) are difficult to introduce incrementally. SkeinDB addresses this by combining:
1. Single-binary operational ergonomics.
2. Adoption-oriented compatibility surfaces.
3. A research-first extension model integrated into the core API.

SkeinDB is designed as one executable that can expose HTTP RPC, admin interfaces, and optional protocol extensions in one process. This reduces orchestration overhead for development, demos, edge deployments, and reproducible experimentation.

The core thesis of this paper is that a database can provide a stable, explicit, web-native control plane (SkeinQL) while remaining a practical foundation for iterative systems research. Instead of introducing one monolithic research feature at a time, SkeinDB organizes innovations into agenda modules with concrete RPC methods, tests, and documentation.

This manuscript contributes:
- A practical architecture for single-binary DB delivery with integrated control plane.
- A concrete cluster control-plane implementation with persistent metadata and shard operations.
- A replication transport design that uses RPC-level write fanout in the current prototype.
- A unified admin UX that surfaces operational and research controls.
- A research-to-implementation mapping across 20 agenda tracks.

## 2. System Goals and Design Principles
SkeinDB is guided by four primary principles:

1. **Operational minimalism:** one executable and direct local startup.
2. **Protocol clarity:** typed, versioned JSON-RPC methods via SkeinQL.
3. **Research modularity:** capabilities implemented as scoped method families.
4. **Progressive compatibility:** MySQL adoption support with migration tooling.

These principles avoid the common split where research systems remain disconnected from operator workflows. In SkeinDB, each major feature has:
- method-level API definitions,
- runnable tests,
- and operator-facing docs/UI touchpoints.

## 3. Architecture Overview
### 3.1 Process Model
A single SkeinDB process hosts:
- HTTP JSON-RPC endpoint (`/api/v1/rpc`),
- embedded admin/console UI,
- optional QUIC endpoint for SkeinQL-over-QUIC,
- internal execution engine and state managers.

### 3.2 API Plane
SkeinQL provides strongly structured method families, including:
- `system.*`, `transport.*`, `stats.*`,
- `schema.*`, `data.*`, `query.*`,
- `cluster.*`,
- and research families (`vector.*`, `dp.*`, `oblivious.*`, `forensic.*`, `migration.*`, `wasm.plan.*`, etc.).

### 3.3 State and Persistence
Runtime settings are persisted in `settings.json`, including cluster metadata (`cluster.state.v1`) and control flags. This keeps control-plane state durable without introducing separate external metadata services in the prototype stage.

## 4. Cluster Control-Plane and Replication
### 4.1 Implemented Cluster Methods
The current implementation includes:
- `cluster.status`
- `cluster.nodes`
- `cluster.join_token.create`
- `cluster.node.join`
- `cluster.node.remove`
- `cluster.replica.promote`
- `cluster.shard.create`
- `cluster.shard.move`
- `cluster.shard.rebalance`

These methods operate on persistent cluster state and are exposed in `system.capabilities.methods`.

### 4.2 Node Identity and Join Workflow
Each node has a stable local node identity and participates in a cluster identified by `cluster_id`. Join tokens are short-lived, role-aware credentials created through `cluster.join_token.create`; nodes are admitted through `cluster.node.join`.

### 4.3 Shard Metadata and Placement
Table-scoped shard metadata is managed in control-plane state. Operators can:
- define shard ownership (`cluster.shard.create`),
- move ownership (`cluster.shard.move`),
- and rebalance placement (`cluster.shard.rebalance`).

Writes are guarded so only the designated primary (global or shard-level) can accept authoritative writes.

### 4.4 Replication Transport (Prototype)
The current transport replicates successful primary writes to replica nodes via SkeinQL RPC fanout. Replication requests are marked with an internal header (`x-skeindb-replication: 1`) to prevent recursive fanout and to allow controlled application on replicas.

Replication counters are tracked in control-plane state and surfaced through:
- `cluster.status.replication`
- `stats.snapshot.cluster.replication`

This provides an immediately testable, developer-friendly transport before introducing full WAL/LSN streaming and CAS object pull.

## 5. Web Admin Experience
SkeinAdmin now includes:
- phpMyAdmin-like workspace organization (overview, SQL, schema, data, cluster, settings, RPC explorer),
- server connect/disconnect profiles,
- cluster action controls (join/promote/shard/move/rebalance),
- direct settings management (`settings.get`, `settings.set`),
- and advanced method access through RPC Explorer.

The UX objective is to ensure that every feature is operable without CLI-only workflows.

## 6. Research Agenda Mapping
SkeinDB tracks 20 research agenda streams (R01–R20). The current implementation includes substantial progress across execution, API, and tooling tracks, with active work in clustering and operations.

### 6.1 Implemented/Operational in prototype scope
- Web-native validation and patching (`query.select` with ETag semantics, `query.patch` deltas).
- QUIC transport path and protocol tests.
- Vector search and indexing primitives.
- Differential privacy aggregates and budget controls.
- Oblivious access policy/explain controls.
- Forensic query/verify/export surfaces.
- Merge policies and wasm merge registry.
- View lifecycle and dependency-aware operations.
- Migration intent/rewrite analysis workflows.
- Cluster control-plane + replication fanout + shard metadata operations.

### 6.2 Remaining deep systems milestones
- Full WAL streaming protocol with LSN semantics.
- CAS object pull and object-manifest replication.
- Read-balancing router and automated failover orchestration.
- Complete energy-aware compaction and formal performance harnesses.

## 7. Validation and Test Strategy
Validation combines:
- unit tests for method behavior and state transitions,
- RPC roundtrip tests,
- QUIC transport integration tests,
- cluster replication integration tests (multi-node spawned process tests).

Standard acceptance pipeline:
- `cargo fmt`
- `cargo clippy`
- `cargo test`

This test-first approach is critical because SkeinDB exposes many method families and control-plane transitions where regressions can be subtle.

## 8. Discussion
### 8.1 Strengths
- High implementation velocity from unified binary + explicit RPC contracts.
- Strong operator visibility through embedded control plane.
- Research extensibility without external orchestration prerequisites.

### 8.2 Limitations
- Current replication transport is RPC fanout, not full WAL/CAS replication.
- Router-level automatic read-balancing is not yet complete.
- Some method families remain experimental and should be treated as evolving contracts.

### 8.3 Threats to Validity
- Prototype benchmarks may not represent tuned production systems.
- In-memory/exploratory components can overstate throughput under constrained workloads.
- Integration tests validate correctness of flows, not full failure-domain behavior (network partitions, split-brain, disk faults).

## 9. Conclusion
SkeinDB demonstrates that a single-binary database can combine practical operator workflows and high-velocity systems research. The integrated SkeinQL control plane, expanded admin experience, and implemented cluster control-plane provide a coherent path from local development to distributed experimentation. The prototype already supports end-to-end cluster operations (join/promote/shard/rebalance) and replication fanout with persistent state and test coverage. Future work will complete WAL/CAS-native replication, routing automation, and broader performance/energy optimization to transition from research scaffold to production-grade distributed operation.

## Acknowledgments
_Optional section. Add contributors, institutions, funding, or infrastructure support._

## References
[1] Kraska, T., et al. "The Case for Learned Index Structures." SIGMOD, 2018.

[2] Neumann, T. "Efficiently Compiling Efficient Query Plans for Modern Hardware." VLDB, 2011.

[3] Haas, A., et al. "Bringing the Web up to Speed with WebAssembly." PLDI, 2017.

[4] Lloyd, W., et al. "Don’t Settle for Eventual: Scalable Causal Consistency." SOSP, 2011.

[5] McSherry, F. "Privacy Integrated Queries." SIGMOD, 2009.

[6] Stefanov, E., et al. "Path ORAM." CCS, 2013.

[7] Gupta, A., and Mumick, I. "Maintenance of Materialized Views." IEEE Data Eng. Bulletin, 1995.

[8] Chaudhuri, S., and Narasayya, V. "AutoAdmin." SIGMOD, 1998.

## Appendix A: Reproducibility Notes
- Build: `cargo build --release`
- Run: `./target/release/skeindb serve --data ./data --http 8080 --mysql 3306`
- Admin UI: `http://127.0.0.1:8080/admin`
- RPC endpoint: `http://127.0.0.1:8080/api/v1/rpc`

## Appendix B: Suggested IJRCOM Submission Checklist
- Add final author names, affiliations, and correspondence.
- Apply IJRCOM template typography/heading constraints in DOC format.
- Verify citation style and numbering against IJRCOM author guidelines.
- Export final PDF with embedded fonts.
