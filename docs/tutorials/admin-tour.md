# Admin console tour

**SkeinAdmin** is the built-in web admin — 26 panels covering schema, data, configuration, clustering, research features, and operations. It ships with the single binary: no separate install, no Node/Electron, no cloud service.

Open it at `http://127.0.0.1:8080/admin` once `skeindb serve` is running.

## Layout

![SkeinAdmin — overview](assets/screenshots/admin-overview.png)

- **Left sidebar** — panel switcher, grouped into Data, Ops, Security, Research, and System.
- **Top bar** — connection info, node status, running server version.
- **Main pane** — the active panel.
- **Status strip** — health, storage size, replica lag, alerts.

## Tour of the key panels

### Dashboard

![Dashboard](assets/screenshots/admin-dashboard.png)

At-a-glance: throughput, QPS, WAL lag, slow queries, connection counts. Small enough to watch on a laptop.

### Schema browser

![Schema browser](assets/screenshots/admin-schema.png)

Tree view of databases → tables → columns / indexes / views. Click a column to see type, nullability, default, and referring indexes.

### Data editor (Easy Viewer)

![Data editor](assets/screenshots/admin-data-editor.png)

Inline database creation, live `CREATE TABLE` preview, cell editing with MVCC version history on the right panel. Useful for early development without dropping into SQL.

### SQL console

![SQL console](assets/screenshots/admin-sql.png)

Dialect-aware: write MySQL, PostgreSQL (partial), or SkeinQL; results render in the same table. Query history, save/share snippets, explain plans.

### Index advisor

![Index advisor](assets/screenshots/admin-index-advisor.png)

The [Index advisor](index-advisor.html) page. Ranked recommendations from workload telemetry, with an "apply" button and an observed-before / expected-after panel.

### Cluster dashboard

![Cluster dashboard](assets/screenshots/admin-cluster.png)

Topology view: nodes, shards, replica lag, join tokens, manual promotion, rolling upgrade co-ordination. See [Clustering](clustering.html).

### Audit WAL viewer

![Audit WAL](assets/screenshots/admin-audit.png)

Tamper-evident log viewer with forensic query language. Verify chain integrity from the UI. See [Audit WAL](audit-wal.html).

### Research panels

Dedicated UIs for research tracks: ETag cache coherence, query coalescing, CDC, Wasm UDFs, oblivious execution, differential privacy budgets, merge policies, delta chains, column snapshots. Each panel surfaces the relevant metrics and toggles — the same data the SkeinQL API exposes, just visual.

## Keyboard shortcuts

| Shortcut | Action |
|---|---|
| `Ctrl` / `Cmd` + `K` | Command palette / fuzzy panel jump |
| `Ctrl` / `Cmd` + `Enter` | Run query in SQL console |
| `?` | Show all shortcuts |
| `g` + `d` | Go to Dashboard |
| `g` + `s` | Go to Schema browser |

## Authentication

By default, SkeinAdmin is served on the same port as SkeinQL. For production, put a reverse proxy in front and configure auth via `admin.auth` in [`skeindb-config.json`](configuration.html). Settings are also editable from the admin console's **Settings → Explorer** panel with live validation.

## Next

- [Clustering](clustering.html)
- [Observability](observability.html)
- [Audit WAL](audit-wal.html)
- [Performance](performance.html)
