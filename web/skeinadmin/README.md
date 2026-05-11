# SkeinAdmin (Standalone Console)

This folder contains the embedded **SkeinAdmin** SPA, a click-first management console with optional expert mode.

Design goals:
- Static deployable SPA (hostable behind IIS/Apache/Nginx)
- Connects to SkeinDB via **SkeinQL HTTP API** (`POST /api/v1/rpc`)
- Supports multiple servers and clusters via connection profiles

Implemented navigation areas:
- **Easy Viewer** (familiar DB/table/row management)
  - Left sidebar with collapsible database/table tree and filter
  - Sub-tabs: Browse, Structure, Insert, Search, New Table, Export, Operations
  - Inline grid editing with per-row Edit/Copy/Delete actions
  - Bulk-delete with check-all, pagination, toast notifications
  - Condition-based search, column-builder create-table, CSV/SQL export
  - Operations: truncate table, drop table, drop database with confirmation
- **Schema** / **Data** / **SQL Workspace**
- **Comprehensive Dashboard** (runtime, storage/dedup stats with bar chart, learned ValueID lookup telemetry, MVCC/compaction, query/cache metrics, auto-refresh; live Top Tables / Slow Query Log / Active Sessions / Index Health cards backed by `information_schema.tables`, `stats.slow_queries`, and `stats.snapshot`)
- **Engine Config** (toggle dedup, compression, encryption, MVCC, delta chains, time travel, compaction, cache, security, replication, CDC, QUIC via checkboxes; compaction scheduler status/policy/pause/resume via `maintenance.compaction.*`)
- **Admin lifecycle controls** (connect/disconnect + graceful `system.shutdown`)
- **Cluster** (join tokens, node join/leave, replica promotion, shard create/move/rebalance)
- **Settings Manager** (`settings.get` / `settings.set`, including `cluster.state.v1`)
- **Time Travel & Replay** (`query.select as_of`, `maintenance.history.*`, `maintenance.replay.*`, edge bundle request/apply/status, bundle download/import, integrity summary)
- **Forensics** (`maintenance.audit_status`, `maintenance.audit_verify`, filtered `forensic.query`, proof-backed verify flow, and `forensic.export` report bundles)
- **Merge & CRDT** (`merge.apply`, `merge.register`, `merge.simulate`, `merge.evaluate`, and `merge.wasm.*` with values-only Wasm module limits)
- **RPC Explorer** (full method-level access)
  - includes CDC cursor helpers such as `cdc.subscribe_table`, `cdc.poll`, `cdc.ack`, and `cdc.close`
- **Research Dashboard**: all R01-R20 tracks link to a concrete panel or one-click RPC template, including R01 learned-index stats, R07 merge evaluation, R14 edge bundles, and R20 compaction scheduling.
- **Migration Assistant** (intent + rewrite preview)
- **NL Lab** (translate/explain/execute workflow)
- **Help & Documentation** (quick start, panel reference, research track index, keyboard/deep-link reference, glossary, doc links, and live search)

Roadmap areas:
- **Index Advisor** + **Index Synthesis** (dependency-driven)
- **Views** (incremental maintenance, refresh mode, evaluation oracle)
- **CDC** dedicated subscriptions page + lag visualization
- **Performance Replay extensions** (timing capture / variance analysis beyond current snapshot-based replay bundles)
- Experimental: **Embeddings**, **NL Query**

For the full console specification, see: `docs/SKEINADMIN.md`.
