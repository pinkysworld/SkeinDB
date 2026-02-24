# SkeinAdmin (Standalone Console)

This folder contains the embedded **SkeinAdmin** SPA, a click-first management console with optional expert mode.

Design goals:
- Static deployable SPA (hostable behind IIS/Apache/Nginx)
- Connects to SkeinDB via **SkeinQL HTTP API** (`POST /api/v1/rpc`)
- Supports multiple servers and clusters via connection profiles

Implemented navigation areas:
- **Easy Viewer** (click-first DB/table/row workflows)
- **Inline grid row editor** (spreadsheet-style row entry + in-place updates)
- **Optional visual row editor** (row-click browsing with WYSIWYG-style form edits)
- **Schema** / **Data** / **SQL Workspace**
- **Server Load & Statistics** (QPS/TPS, latency, compaction, cache)
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
