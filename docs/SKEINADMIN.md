# SkeinAdmin (Standalone Management Console)

Status: Implemented embedded admin panel + active roadmap
Last updated: 2026-02-24

SkeinAdmin is a **standalone** management console for SkeinDB.
It is intentionally separate from the SkeinDB server binary (similar in spirit to phpMyAdmin),
so administrators can host it independently (IIS/Apache/Nginx/static hosting) and manage
**multiple SkeinDB servers or clusters** from one UI.

SkeinDB now ships an embedded SkeinAdmin build at:
- `/admin` for full control-plane administration
- `/console` for SQL/workspace-first operation

The same UI bundle powers both routes, with mode-aware navigation and controls.

Recent UI updates:
- Overview includes live dedup/storage metrics (`dedup_ratio`, logical vs unique bytes, interned values).
- Connect/disconnect and profile workflows are shared across admin and console routes.
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

4. **phpMyAdmin-class workflows**
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
- Authentication uses Bearer tokens (recommended).

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

---

## 4) UI sections (navigation)

### 4.1 Overview
- Server name/version
- Uptime
- Storage size
- Role: standalone / primary / replica / router

### 4.2 SQL Workspace
- SQL editor with tabs
- History
- Saved queries
- Results grid
- EXPLAIN plan viewer

### 4.3 Schema Browser
- Databases
- Tables
- Columns
- Indexes
- DDL view: "SHOW CREATE TABLE" equivalent

### 4.4 Data Browser
- Table browse (paging/sort/filter)
- Row edit/create/delete
- CSV import/export

### 4.5 Users & Privileges
- Create user
- Reset credentials
- Grant/revoke privileges
- Show grants

### 4.6 Maintenance
- Checkpoint
- Compact/vacuum
- Compaction policy (adaptive scheduler) + pause/resume
- Snapshot management (column snapshots)
- Audit verification (hash-chained WAL)

### 4.6.1 Time travel & replay
- Point-in-time query runner (as_of)
- Replay bundle export/import/verify

### 4.7 Server Load & Statistics
(See docs/OBSERVABILITY.md)
- CPU, memory, disk, network
- QPS, active sessions
- Latency p50/p95/p99
- Cache hit rates (ETag 304 hit rate)
- Compaction progress and backlog
- Dedup ratio
- Autoparameterization hit rate
- CDC subscriptions and lag (if enabled)

### 4.8 Cluster Management
(See docs/CLUSTERING.md)
- Node list (health, role, lag)
- Add node / remove node
- Promote replica
- Shards and placement
- Rebalance

### 4.9 Security and Encryption
- Encryption mode (ENC_OFF / ENC_RANDOM / ENC_MLE_DB)
- Key rotation and re-encryption progress

### 4.10 CDC Subscriptions
- Table subscriptions
- Prepared-query subscriptions
- Lag and backlog


### 4.11 Index Advisor
(See docs/INDEX_ADVISOR.md)
- Suggested indexes (ranked)
- One-click apply (with progress)
- Dismiss / snooze suggestions
- Before/after metrics for impacted queries



### 4.12 Views (Incremental Maintenance)
(See `docs/research_agenda/R08_*`)
- Create/drop views
- Show view freshness/lag
- Trigger refresh (incremental or full)
- Show dependency graph edges (what base tables feed the view)

### 4.13 Forensics (Verifiable WAL Queries)
(See `docs/AUDIT_WAL.md` and `docs/research_agenda/R06_*`)
- Run forensic queries over the hash-chained WAL
- Verify proofs (completeness/inclusion)
- Export signed forensic reports

### 4.14 Migration Assistant (MySQL → SkeinQL)
(See `docs/TELEMETRY_AND_MIGRATION.md` and `docs/research_agenda/R17_*`)
- Compatibility report (unsupported features)
- Intent inference: detect patterns like pagination, polling, soft deletes
- Rewrite previews: before/after SkeinQL migration hints
- Exportable rewrite reports (JSON/Markdown/HTML) + copy-to-clipboard

### 4.15 NL Query (Experimental)
(See `docs/research_agenda/R12_*`)
- Natural language prompt workspace
- `ai.nl.translate` preview + query JSON editor
- `ai.nl.explain` summary + preview rows + approval token
- `ai.nl.execute` gated execution using approval token
- Suggested SkeinQL-native rewrites (cursor API, CDC subscribe, etc.)

### 4.15 Embeddings
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
- SA09: Index Advisor page (advisor.*)
- SA10: Time travel + replay bundle UI (query.select as_of + maintenance.replay.*)
- SA11: Encryption + key rotation UI (settings.encryption + status/progress)
- SA12: CDC subscriptions UI (cdc.*) + lag visualization
- SA13: Compaction scheduler policy UI (maintenance.compaction.*)
- SA14: Autoparameterization and plan-cache widgets
- SA15: Forensics page (forensic.*) + proof verification UI
- SA16: Views page (view.*) + dependency visualization
- SA17: Migration Assistant (telemetry + intent inference) + exportable report
- SA18: Embeddings playground (vector.*) + index status
- SA19: NL Query page (ai.nl.*) with verification gate
