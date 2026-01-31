# SkeinAdmin (Standalone Console)

This folder is a placeholder scaffold for **SkeinAdmin**, a standalone phpMyAdmin-like management console.

Design goals:
- Static deployable SPA (hostable behind IIS/Apache/Nginx)
- Connects to SkeinDB via **SkeinQL HTTP API** (`POST /api/v1/rpc`)
- Supports multiple servers and clusters via connection profiles

Planned navigation areas (see specs):
- **Schema** / **Data** / **SQL Workspace**
- **Server Load & Statistics** (QPS/TPS, latency, compaction, cache)
- **Cluster** (replicas, join tokens, shard placement)
- **Index Advisor** + **Index Synthesis** (dependency-driven)
- **Views** (incremental maintenance)
- **CDC** subscriptions
- **Forensics** (hash-chained WAL verification + proofs)
- **Replay** (time travel, reproducible replays, performance replays)
- **Migration Assistant** (MySQL → SkeinQL intent inference)
- Experimental: **Embeddings**, **NL Query**

For the full console specification, see: `docs/SKEINADMIN.md`.
