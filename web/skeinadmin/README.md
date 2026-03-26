# SkeinAdmin (Standalone Console)

This folder contains the embedded **SkeinAdmin** SPA, a click-first management console with optional expert mode.

Design goals:
- Static deployable SPA (hostable behind IIS/Apache/Nginx)
- Connects to SkeinDB via **SkeinQL HTTP API** (`POST /api/v1/rpc`)
- Supports multiple servers and clusters via connection profiles
- Persists SQL dialect mode per profile/workspace (`native`, `mysql`, `postgres`) for `sql.exec`

Implemented navigation areas:
- **Easy Viewer** (familiar DB/table/row management)
  - Left sidebar with collapsible database/table tree and filter
  - Sub-tabs: Browse, Structure, Insert, Search, New Table, Export, Operations
  - Inline grid editing with per-row Edit/Copy/Delete actions
  - Bulk-delete with check-all, pagination, toast notifications
  - Condition-based search, column-builder create-table, CSV/SQL export
  - Operations: truncate table, drop table, drop database with confirmation
- **Schema** / **Data** / **SQL Workspace**
- **Dialect-aware SQL Workspace** (native + MySQL/Postgres compatibility modes over HTTP `sql.exec`)
- **Comprehensive Dashboard** (runtime, storage/dedup stats with bar chart, MVCC/compaction, query/cache metrics, auto-refresh)
- **Engine Config** (toggle dedup, compression, encryption, MVCC, delta chains, time travel, compaction, cache, security, replication, CDC, QUIC via checkboxes)
- **Admin lifecycle controls** (connect/disconnect + graceful `system.shutdown`)
- **Cluster** (join tokens, node join/leave, replica promotion, shard create/move/rebalance)
- **Settings Manager** (`settings.get` / `settings.set`, including `cluster.state.v1`)
- **RPC Explorer** (full method-level access)
- **Migration Assistant** (intent + rewrite preview)
- **NL Lab** (translate/explain/execute workflow)

Roadmap areas:
- **Index Advisor** + **Index Synthesis** (dependency-driven)
- **Views** (incremental maintenance)
- **CDC** subscriptions
- **Forensics** (hash-chained WAL verification + proofs)
- **Replay** (time travel, reproducible replays, performance replays)
- Experimental: **Embeddings**, **NL Query**

For the full console specification, see: `docs/SKEINADMIN.md`.
