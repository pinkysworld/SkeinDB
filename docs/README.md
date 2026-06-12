# SkeinDB Documentation

Last updated: 2026-05-27

This folder contains the design notes, specifications, and operator/developer documentation for **SkeinDB**.

The repo is intentionally written so you can read it in two ways:
- *As a working prototype*: build/run it and use SkeinAdmin.
- *As a research scaffold*: start from the primitives (ValueIDs, MVCC, SkeinQL, ETags) and extend.

## Recommended reading order

1) **Getting started**
   - `GETTING_STARTED.md` (build, run, first commands)
   - `CONFIGURATION.md` (ports, data dirs, standalone binary behavior)
   - includes storage/dedup `stats.snapshot` quick check

2) **Core APIs**
   - `API_REFERENCE.md` (runtime method families, transports, result formats, client checklist)
   - `SKEINQL.md` (JSON-RPC control plane spec)
   - `MYSQL_COMPAT.md` (MySQL protocol surface and current coverage)
   - `PG_COMPAT.md` (current PostgreSQL wire-protocol baseline and open gaps)
   - `SKEINADMIN.md` (embedded web console)
   - `../SUPPORT.md` (project support / sponsorship options)

3) **Web-native consistency & traffic reduction**
   - `ETAG_VALIDATORS.md` (ETags, If-None-Match)
   - `QUERY_PATCH.md` (query-scoped deltas)
   - `TRAFFIC_REDUCTION.md` (overview of protocol + transport techniques)
   - `CDC_CHANGEFEED.md` (dependency-driven change feeds, including polling/SSE/WebSocket delivery and query dependency invalidation)

4) **Storage + security**
   - `ON_DISK_FORMAT.md` (formats + record versions)
   - `DELTA_VALUES.md` (MVCC delta chains)
   - `AUDIT_WAL.md` (tamper-evident audit logging)
   - `WASM_UDFS.md` (sandboxed extensions)
   - includes table row `format_version: 2` ValueID ref encoding (prototype)

5) **Privacy, Security & Compliance**
   - `CONVERGENT_ENCRYPTION.md` (dedup-preserving convergent encryption + key rotation)
   - `OBLIVIOUS_EXECUTION.md` (oblivious policies, padded execution, DP via R04)
   - `AUDIT_WAL.md` (forensic queries + proof bundles)
   - `ETAG_VALIDATORS.md` + `TRAFFIC_REDUCTION.md` (cache coherence, causal ETags)
   - See also the Privacy lab and Forensics panels in SkeinAdmin.

6) **Advanced Storage, Views & Pipelines**
   - `INCREMENTAL_VIEWS.md` (materialized view maintenance, dependency graphs, R08)
   - `DELTA_VALUES.md` + `COLUMN_SNAPSHOTS.md` (delta chains, hybrid snapshots)
   - `CDC_CHANGEFEED.md` (query-aware changefeeds with view/CTE expansion)
   - `MERGE_FUNCTIONS.md` (CRDT merges + Wasm, offline queue)
   - `CAS_REPLICATION.md` + `TIME_TRAVEL_REPLAY.md` (CAS moves, MVCC time travel, replay bundles)
   - `INDEX_ADVISOR.md` (self-tuning synthesis + evaluate benchmarks, R16)

7) **Clustering & operations**
   - `CLUSTERING.md` (cluster configuration and goals)
   - `OBSERVABILITY.md` (server load / stats endpoint)
   - `RELEASE_PACKAGING.md` (tag-driven release assets, Homebrew formula rendering, optional apt signing)
   - `TELEMETRY_AND_MIGRATION.md` (compatibility telemetry + migration hints)
   - `COMPACTION_SCHEDULER.md` (including energy-aware R20)

8) **Research roadmap**
   - `RESEARCH_AGENDA.md` (20-track research agenda + priorities)
   - `TRUE_STATUS_MATRIX.md` (short current truth snapshot for compatibility, partial core phases, and research maturity)
   - `RESEARCH_BACKLOG.md` (task inventory for the research tracks)
   - `PROJECT_BACKLOG.md` (task inventory for the core roadmap)
   - `research_agenda/` (prioritized research directions)
   - `site/index.html` (generated docs site with architecture and paper summary)

## Contributing docs

If you add a new feature, please also add one of:
- a short design note under `docs/`
- or a research memo under `docs/research_agenda/`

Keep docs readable and concrete:
- include message schemas
- include examples
- call out what is implemented vs planned
