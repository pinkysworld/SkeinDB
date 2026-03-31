# SkeinDB Documentation

Last updated: 2026-03-31

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
   - `SKEINQL.md` (JSON-RPC control plane spec)
   - `MYSQL_COMPAT.md` (MySQL protocol surface and current coverage)
   - `PG_COMPAT.md` (current PostgreSQL wire-protocol baseline and open gaps)
   - `SKEINADMIN.md` (embedded web console)
   - `../SUPPORT.md` (project support / sponsorship options)

3) **Web-native consistency & traffic reduction**
   - `ETAG_VALIDATORS.md` (ETags, If-None-Match)
   - `QUERY_PATCH.md` (query-scoped deltas)
   - `TRAFFIC_REDUCTION.md` (overview of protocol + transport techniques)
   - `CDC_CHANGEFEED.md` (dependency-driven change feeds)

4) **Storage + security**
   - `ON_DISK_FORMAT.md` (formats + record versions)
   - `DELTA_VALUES.md` (MVCC delta chains)
   - `AUDIT_WAL.md` (tamper-evident audit logging)
   - `WASM_UDFS.md` (sandboxed extensions)
   - includes table row `format_version: 2` ValueID ref encoding (prototype)

5) **Clustering & operations**
   - `CLUSTERING.md` (cluster configuration and goals)
   - `OBSERVABILITY.md` (server load / stats endpoint)
   - `TELEMETRY_AND_MIGRATION.md` (compatibility telemetry + migration hints)

6) **Research roadmap**
   - `RESEARCH_AGENDA.md` (20-track status matrix + priorities)
   - `TRUE_STATUS_MATRIX.md` (runtime reality sync: implemented vs prototype vs planned)
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
