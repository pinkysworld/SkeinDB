# SkeinAdmin (Standalone Management Console)

Status: Implemented embedded admin panel + active roadmap
Last updated: 2026-05-11 (v0.3.16)

SkeinAdmin is a **standalone** management console for SkeinDB.
It is intentionally separate from the SkeinDB server binary,
so administrators can host it independently (IIS/Apache/Nginx/static hosting) and manage
**multiple SkeinDB servers or clusters** from one UI.

SkeinDB now ships an embedded SkeinAdmin build at:
- `/admin` for full control-plane administration
- `/console` for SQL/workspace-first operation

The same UI bundle powers both routes, with mode-aware navigation and controls.

## In-app Help Center (v0.3.8)

The console now includes a dedicated **Help & Docs** panel reachable from the
left nav, the top tab strip, the topbar `? Help` button, and the `?` keyboard
shortcut. The Help Center contains:

- A 5-step quick-start checklist (connect, pick a database, browse/design, run
  queries, operate).
- A panel reference table that lists every panel with its purpose, key actions,
  and a one-click "Open" jump button.
- A research-track index covering all twenty R01-R20 tracks with their current
  hardness state (`hardened` vs `prototype`) and primary RPC methods.
- Keyboard-shortcut reference and a deep-link hash table (`#<panel>`).
- Direct links to the canonical documentation site (Getting Started, SkeinQL,
  API Reference, MySQL/PG compatibility, Time Travel & Replay, CDC, Index
  Advisor, research backlog, and the True Status Matrix).
- A live-filter search box that scopes both the panel-reference and the
  research-track tables.
- A glossary of recurring terms (SkeinQL, ValueID, ETag chain, replay bundle,
  CDC subscription, prepared query, audit chain, edge bundle, differential
  privacy, oblivious policy).
- A support section with direct links to the GitHub issue tracker, support
  policy, and commercial licensing.

Recent UI updates:
- **v0.3.4 polish**: live wiring for the previously-stubbed Overview cards (Top Tables / Slow Query Log / Active Sessions / Index Health) via `information_schema.tables`, `stats.slow_queries`, and `stats.snapshot`; Security panel response-shape fixes for create/list/top-queries flows; auto-refresh on Overview/Security tab switches; Active Sessions labels aligned with `stats.snapshot` (`Sessions` / `Open Txns` / `Avg Latency`).
- **Overview dashboard** now shows comprehensive stats: runtime (uptime, CPU, RSS, QPS/TPS, open txns, connections), storage & deduplication (ratio, savings %, logical/unique bytes, interned values, total rows/tables, disk/WAL size, visual bar chart), MVCC & compaction (versions, delta chains, L0 files, stall rate), query & cache (hit %, slow queries, avg latency, ETag hits, coalesced). Auto-refresh toggle (5s).
- **Research runtime wiring**: R01 learned ValueID lookup histograms/model reports appear in Overview via `stats.snapshot`; R14 edge bundles are now first-class Replay panel controls; R20 compaction scheduler policy/status/pause/resume are first-class Engine panel controls.
- **Privacy panel DP evaluation**: the R04 card now exposes `dp.evaluate` with epsilon-grid, trials, seed, mechanism, and bounds controls, plus typed `dp.aggregate`, `dp.budget.*`, and `dp.audit.log` calls. The result metadata now includes per-aggregate sensitivities and `privacy_etag` validators for privacy-aware cache inspection.
- **Privacy panel oblivious evaluation**: the R05 card now uses the runtime policy schema (`level`, `pad_to_multiple`, `target_rows`, `dummy_value_lookups`, `shuffle`), sends nested table payloads for get/set/explain, and exposes `oblivious.evaluate` for deterministic leakage and overhead reports.
- **Merge & CRDT R07 hardening**: the Merge panel now sends typed `merge.apply`, `merge.register`, `merge.simulate`, and `merge.evaluate` payloads; exposes `expected_etag`, `min_causality`, current/incoming row editors, workload-case evaluation controls, and values-only Wasm module limits; and uses `module_id` for Wasm register/drop so the UI matches the runtime RPC schema.
- **Views R08 hardening**: the Views panel now exposes refresh mode selection, `view.evaluate` iterations, status/dependency summaries, and typed `view.create`, `view.refresh`, `view.evaluate`, `view.status`, `view.drop`, and `view.explain_deps` calls for incremental-vs-full correctness review.
- **v0.3.16 help/status sync**: the in-app research index now describes R09 as hardened for QUIC framing, prepared-query streams, 0-RTT write rejection, and rebind coverage while keeping comparative p99 benchmarking listed as follow-up work.
- **Engine Config panel** for toggling engine features via simple checkboxes: deduplication, compression, encryption, MVCC, delta chains, time travel, auto compaction, energy-aware scheduling, query cache, coalescing, autoparameterization, audit WAL, differential privacy, oblivious execution, replication, CDC, QUIC transport. Load/save/reset with `settings.set`.
- Connect/disconnect and profile workflows are shared across admin and console routes.
- Admin topbar includes a guarded **Shutdown** action (`system.shutdown`) for graceful server stop.
- `/admin` now includes a **phpMyAdmin-inspired Easy Viewer** with left sidebar database/table tree, sub-tabs (Browse/Structure/Insert/Search/New Table/Export/Operations), inline row editing, toast notifications, and confirmation dialogs.
- Easy Viewer supports inline grid editing for spreadsheet-style row entry, copy, update, and batch delete.
- Easy Viewer includes form-based insert, column-builder create-table, search with conditions, CSV/SQL export, and table/database operations (truncate, drop). Destructive flows now unwrap RPC errors before showing success, and generated SQL/export statements quote identifiers so non-simple names do not break the “easy” path.
- The Security panel now uses the live token RPC payloads correctly, auto-populates the active Bearer token when you create one, and reflects the server’s persisted token inventory instead of stale in-memory UI assumptions.
- User creation now requires a real password and provisions a stored DB login that MySQL and PostgreSQL wire clients can authenticate with.
- `/console` remains workspace-first, while `/admin` keeps full control-plane navigation.

---

## 1) Design goals

1. **Separate deployment**
   - SkeinAdmin is built as a web application that can be hosted anywhere.
   - It connects to SkeinDB over HTTP(S) using SkeinQL and management endpoints.

2. **Multi-target**
   - One SkeinAdmin instance can manage many servers and many clusters.
   - The UI supports connection profiles.

3. **No MySQL requirement**
   - SkeinAdmin should not rely on browser-to-MySQL connectivity.
   - All operations use SkeinQL HTTP endpoints.

4. **GUI-first administration workflows**
   - Execute SQL
   - Browse schema
   - Browse/edit tables
   - Import/export (CSV, SQL dump)
   - Manage users/privileges

5. **Cluster-first administration**
   - Add/remove nodes
   - Promote replica
   - View replication lag and health
   - Configure sharding / placement rules

6. **Observability built-in**
   - Server load and performance dashboard
   - Query statistics, slow queries, compaction status, dedup ratio

---

## 2) Deployment models

### 2.1 Static SPA (recommended)

- SkeinAdmin is a static single-page application (SPA).
- It talks directly to the SkeinDB HTTP API.

Requirements:
- SkeinDB must enable CORS for allowed origins.
- Authentication uses Bearer tokens (recommended). Persisted API tokens created in the Security panel now protect the HTTP RPC/API surface as soon as at least one active token exists.

### 2.2 Reverse-proxy hosted

- Host the SPA behind IIS/Apache/Nginx.
- Optionally reverse-proxy `/api/` to a specific SkeinDB server.

### 2.3 Air-gapped admin workstation

- SkeinAdmin can be served from local files (or a tiny local server) and connect to a
  SkeinDB node reachable over LAN.

### 2.4 Embedded static assets (single-binary mode)

Even though SkeinAdmin is designed to be hostable as a standalone web app, SkeinDB SHOULD also be able to serve the built SkeinAdmin assets itself to preserve a one-binary deployment experience.

Recommended behavior:
- SkeinDB serves SkeinAdmin at: `/admin`
- All API calls still go to `/api/v1/...` on the same origin
- Port selection is controlled by SkeinDB's `--http` flag

This mode is useful for:
- developer environments
- small on-prem deployments
- edge devices



---

## 3) Connection profiles

A profile stores:
- `name`
- `base_url` (e.g. `https://db1.example.com:8080`)
- auth method
- optional: "cluster alias" for grouping nodes

Security note:
- Do not store raw tokens unencrypted in local storage.
- Prefer short-lived tokens with refresh or manual paste.
- API token secrets are only returned once at creation time; list responses no longer echo the raw secret.

---

## 4) UI sections (navigation)

### 4.1 Overview (Dashboard)
- **Server Info**: version, SkeinQL version, transport mode, ping latency, method count, database count
- **Runtime**: uptime (human-readable), CPU %, RSS memory, QPS/TPS, open transactions, connections
- **Storage & Deduplication**: dedup ratio, dedup enabled status, saved bytes, logical/unique bytes, interned/unique values, savings %, total rows, total tables, disk size, WAL size, visual bar chart showing unique vs saved breakdown
- **Learned ValueID Index (R01)**: lookup sample count, hot-prefix histogram summary, model shift, built/pending state, segment count, key count, max error, max search window, and model/fallback byte estimate from `stats.snapshot.storage.learned_index`
- **MVCC & Compaction**: MVCC versions, delta chains, compaction runs, compaction status, L0 files, stall rate
- **Query & Cache**: cache hit %, cache size, slow queries, avg latency, ETag hits, coalesced queries
- **Auto-refresh**: manual refresh button + auto-refresh toggle (5s interval)
- **Profiles**: save/load/delete connection profiles
- **Feature Center**: quick-launch cards for all major panels

### 4.2 Easy Viewer
- **Left sidebar** with collapsible database/table tree, filter input, New DB button, and reload
- **Inline database creation** from the sidebar with guided validation instead of browser prompts
- **Breadcrumb navigation** showing Server › Database › Table context
- **Sub-tabs** per table: Browse, Structure, Insert, Search, Query Builder, New Table, Design (WYSIWYG), Export, Operations, SQL
- **Browse tab**: paginated data grid with per-row Edit/Copy/Delete buttons, inline editing, check-all bulk delete, configurable rows-per-page
- **Structure tab**: column listing with type, nullable, and primary-key info
- **Insert tab**: form-based row insert with labeled fields per column and required-field validation before RPC submit
- **Search tab**: condition-based search with column/operator/value fields
- **New Table tab**: column builder with name, type, nullable, PK checkboxes plus live SQL preview and duplicate/identifier checks before create
- **Design (WYSIWYG) tab**: load an existing table, edit columns inline (rename / retype / nullable / default / auto-increment / drop), preview the auto-generated `ALTER TABLE` plan (`ADD COLUMN` / `DROP COLUMN` / `RENAME COLUMN` / `MODIFY COLUMN` / `CHANGE COLUMN`), and apply changes one statement at a time via `sql.exec`. The run halts on the first error and the diff is reset after a successful apply.
- **Export tab**: export table data as CSV or SQL, plus structure-only SQL export
- **Operations tab**: truncate table, drop table, and drop database with confirmation dialogs
- **Toast notifications** for success, error, and info feedback
- Responsive layout: sidebar collapses on narrow viewports

### 4.3 SQL Workspace
- SQL editor with tabs
- History
- Saved queries
- Results grid
- EXPLAIN plan viewer

### 4.4 Schema Browser
- Databases
- Tables
- Columns
- Indexes
- DDL view: "SHOW CREATE TABLE" equivalent

### 4.5 Data Browser
- Table browse (paging/sort/filter)
- Row edit/create/delete
- CSV import/export

### 4.6 Engine Config
- **Storage Engine**: deduplication on/off, compression, encryption at rest, storage mode (`json` / `segment` / `hybrid`)
- **MVCC & Versioning**: MVCC toggle, delta-chained values, time travel, version retention days
- **Compaction**: auto compaction, energy-aware scheduling (R20), max L0 files threshold
- **Compaction Scheduler (R20)**: live scheduler status, policy update, pause, and resume controls backed by `maintenance.compaction.status`, `maintenance.compaction.set_policy`, `maintenance.compaction.pause`, and `maintenance.compaction.resume`
- **Cache & Query**: query cache, query coalescing, autoparameterization, cache size (MB)
- **Audit & Security**: tamper-evident WAL (R06), differential privacy (R04), oblivious execution (R05)
- **Replication & CDC**: replication toggle, CDC changefeeds, QUIC transport (R09)
- Load/save/reset controls with immediate feedback via `settings.set`

### 4.6a Settings Manager
- Live settings editor backed by `settings.get`, `settings.set`, and `settings.list`
- Preset keys for common runtime knobs (cluster state, research config, storage mode, cache/coalescing/autoparam, CDC, QUIC)
- Capabilities / method explorer with jump-to-RPC shortcuts
- Quick pulls for transport status, feature flags, and workload feature telemetry
- Research config dashboard with toggle + JSON editor for all 20 research tracks

### 4.7 Users & Privileges
- Create user
- Reset credentials
- Grant/revoke privileges
- Show grants

### 4.8 Maintenance
- Checkpoint
- Compact/vacuum
- Compaction policy (adaptive scheduler) + pause/resume
- Snapshot management (column snapshots)
- Audit verification (hash-chained WAL)
- Graceful shutdown trigger (admin action that checkpoints and marks cluster node offline)

### 4.8.1 Time travel & replay
- Point-in-time query runner built on `query.select as_of`, with ISO/epoch timestamp entry, seeded query JSON from the selected table, and inline result-grid rendering.
- History retention dashboard built on `maintenance.history.status`, showing per-table live/tombstone/purgeable counts plus the effective retention policy.
- History retention policy save + GC controls via `maintenance.history.set_policy` and `maintenance.history.gc`.
- Replay bundle export/download/import/integrity flows built on `maintenance.replay.export`, `maintenance.replay.import`, and `maintenance.replay.run`, including primary-key redaction controls, session-local replay workspace tracking, and checksum summaries.
- Edge bundle request/apply/status workflows built on `edge.bundle.request`, `edge.bundle.apply`, and `edge.bundle.status`, including redaction mode, sequence windows, and query route checks.

### 4.9 Server Load & Statistics
(See docs/OBSERVABILITY.md)
- CPU, memory, disk, network
- QPS, active sessions
- Latency p50/p95/p99
- Cache hit rates (ETag 304 hit rate)
- Compaction progress and backlog
- Dedup ratio
- Autoparameterization hit rate
- CDC subscriptions and lag (if enabled)

### 4.10 Cluster Management
(See docs/CLUSTERING.md)
- Node list (health, role, lag)
- Add node / leave node / remove node
- Promote replica
- Shards and placement
- Rebalance

### 4.11 Security and Encryption
- API token create/list/revoke panel with modal confirmations, secret-once display, and real HTTP bearer enforcement
- Token roles: `admin`, `read_write`, and `read_only`; read-only tokens are blocked from mutating RPC methods
- DB-user management panel with password-backed user creation, partial per-database revoke, and MySQL/PG login compatibility
- Dedicated Security panel entry in the main navigation and top tab bar
- Encryption mode (ENC_OFF / ENC_RANDOM / ENC_MLE_DB)
- Key rotation and re-encryption progress

### 4.12 CDC Subscriptions
- Create table subscriptions via `cdc.subscribe_table`
- Poll / ACK / close session-local handles via `cdc.poll`, `cdc.ack`, and `cdc.close`
- Inspect lag for the currently tracked browser-session subscriptions via `next_offset - acked_offset`
- Prepared-query subscriptions remain planned work

### 4.13 Index Advisor
(See docs/INDEX_ADVISOR.md)
- Ranked index suggestions from live advisor telemetry
- Apply / dismiss actions wired to `advisor.index_synthesize`, `advisor.apply_index`, `advisor.dismiss`, and `advisor.history`
- Embedded history log for prior advisor actions
- Observed-before and expected-after scan report for each suggestion
- Online build progress remains backlog work



### 4.13 Views (Incremental Maintenance)
(See `docs/research_agenda/R08_*`)
- Create/drop views
- Show view freshness/lag
- Trigger refresh (incremental or full)
- Show dependency graph edges (what base tables feed the view)

### 4.14 Forensics (Verifiable WAL Queries)
(See `docs/AUDIT_WAL.md` and `docs/research_agenda/R06_*`)
- Inspect chain length, checkpoint anchors, and last verified time via `maintenance.audit_status`
- Run full-chain verification via `maintenance.audit_verify` and surface the persisted `last_verified_ms`
- Run filtered forensic queries over the hash-chained WAL by DB, table, operation, id bounds, and SkeinForensic JSON filter
- Proof-verify the current query slice by first fetching records and then calling `forensic.verify` with the returned `records` and boundary hash
- Export `skein.forensic.bundle.v1` report bundles with query manifest, proof, records, and verification summary

### 4.15 Migration Assistant (MySQL → SkeinQL)
(See `docs/TELEMETRY_AND_MIGRATION.md` and `docs/research_agenda/R17_*`)
- Compatibility report (unsupported features)
- Intent inference: detect patterns like pagination, polling, soft deletes
- Rewrite previews: before/after SkeinQL migration hints
- Exportable rewrite reports (JSON/Markdown/HTML) + copy-to-clipboard

### 4.16 NL Query (Hardened Research Baseline)
(See `docs/research_agenda/R12_*`)
- Natural language prompt workspace
- `ai.nl.translate` preview + query JSON editor
- `ai.nl.explain` summary + preview rows + approval token
- `ai.nl.execute` gated execution using approval token
- Re-explain/edit loop before execution, backed by approval-token recomputation

### 4.17 Embeddings
(See `docs/research_agenda/R10_*`)
- Ingest embedding vectors
- Build / monitor ANN index health
- Playground for hybrid queries (filters + ANN order-by)

### 4.16 Natural Language Queries
(See `docs/research_agenda/R12_*`)
- NL-to-SkeinQL translation (read-only by default)
- Explanation + dry-run preview
- Explicit confirmation gate for write queries
---

## 5) API usage

SkeinAdmin uses SkeinQL methods only.

Minimum required methods:
- system.version
- system.capabilities
- system.shutdown (optional but recommended for controlled operations)
- schema.list_databases / list_tables / describe_table
- query.select
- sql.exec (optional, for power users)
- stats.snapshot
- stats.top_queries
- cluster.status (if cluster enabled)

---

## 6) Security

Recommended baseline:
- HTTPS only
- Bearer token auth
- RBAC in SkeinDB (admin vs read-only vs operator)
- CSRF protection is handled by token + same-site policy (if cookies are used)

SkeinAdmin should support:
- read-only mode profiles
- audit log (who executed which admin actions)

---

## 7) Backlog (SkeinAdmin)

- SA01: Create SkeinAdmin SPA scaffold (web/skeinadmin)
- SA02: Connection profile UI + token handling
- SA03: SQL Workspace (execute via sql.exec)
- SA04: Schema Browser (schema.*)
- SA05: Data Browser (query.select + data.*)
- SA06: Users UI (admin.*)
- SA07: Stats dashboard (stats.*)
- SA08: Cluster dashboard (cluster.*)
- SA09: Index Advisor page (advisor.*) — implemented prototype; online build progress remains backlog
- SA10: Time travel + replay bundle UI (query.select as_of + maintenance.replay.*) — implemented with the dedicated `Time Travel & Replay` panel, including `maintenance.history.*` retention controls and replay integrity summaries.
- SA11: Encryption + key rotation UI (settings.encryption + status/progress)
- SA12: CDC subscriptions UI (cdc.*) + lag visualization — implemented for table subscriptions with session-local handle tracking; query subscriptions and streaming remain backlog work
- SA13: Compaction scheduler policy UI (maintenance.compaction.*) — implemented with status, policy update, pause, and resume controls in Engine Config
- SA14: Autoparameterization and plan-cache widgets
- SA15: Forensics page (`maintenance.audit_*`, `forensic.*`) + proof verification UI
- SA16: Views page (view.*) + dependency visualization
- SA17: Migration Assistant (telemetry + intent inference) + exportable report
- SA18: Embeddings playground (vector.*) + index status
- SA19: NL Query page (ai.nl.*) with verification gate
