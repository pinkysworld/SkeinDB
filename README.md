# SkeinDB

Last updated: 2026-03-31

SkeinDB is a **single-executable** database engine scaffold that targets two goals at once:

1) **Adoption:** drop-in **MySQL compatibility** plus an early **PostgreSQL compatibility baseline** so existing applications can connect with minimal change.
2) **Research extensibility:** a clean, web-native control plane (**SkeinQL**) and a set of novel primitives (ETag-driven cache coherency, query-scoped patches, delta-chained MVCC, hash-chained WAL, sandboxed Wasm extensions) intended to make systems research easier to prototype and evaluate.

The repository is written so you can:
- run SkeinDB as a portable binary (HTTP + admin console)
- use MySQL tools today and exercise early PostgreSQL clients against the current PG baseline
- extend the engine via well-scoped crates and specs

> Implementation note
> The current execution engine is a small in-memory/JSON-backed prototype meant to make the APIs usable today.
> The paper-aligned ValueID/MVCC/LSM engine is represented as specs + backlog tasks and can be incrementally implemented.

![SkeinDB architecture](docs/figures/architecture.png)

---

## Highlights

- **Single-binary deployment:** copy one executable; pick ports; run.
- **MySQL adoption layer:** MySQL protocol surface, WordPress-class admin compatibility including Users/Site Health query coverage, installer seed-query regressions, and a clean live admin smoke across core screens (theme-owned `nav-menus` / `widgets` limitations aside), plus migration/telemetry tooling.
- **PostgreSQL adoption layer (partial baseline):** PostgreSQL v3 wire protocol on port 5432 with trust/cleartext auth, managed DB-user password auth, SSL rejection, simple query execution, common startup/bootstrap probes (`SELECT version()`, `current_database()`, `current_schema()`, `SHOW server_version`, `current_setting(...)`), simple-query failed-transaction `ReadyForQuery(E)` handling, and extended-protocol stubs. SCRAM, `pg_catalog`, and broader PG dialect parity remain open.
- **SkeinQL (native API):** JSON-RPC control plane for modern apps.
- **Web-native consistency:** ETags + If-None-Match as first-class query validators, including cacheable prepared-query GETs.
- **Traffic reduction:** `query.patch` deltas, patch caching/coalescing, dictionary encoding (`skeinpack_v1`).
- **MVCC extensions:** delta-chained value versions.
- **Dedup visibility:** live storage dedup metrics in `stats.snapshot` and SkeinAdmin overview.
- **Configurable row persistence (prototype):** table row files support ValueID-backed JSON (`.json`), binary row segments (`.rseg`, now the default), or hybrid dual-write mode via `--storage-mode json|segment|hybrid`.
- **Security extensions:** hash-chained WAL for tamper evidence.
- **14 hardened research tracks:** R02-R11 and R13-R16 are hardened with runtime evidence and integration tests; see `docs/TRUE_STATUS_MATRIX.md`.
- **Sandboxed compute:** Wasm UDFs with capability-based access.
- **Wasm operators (experimental):** plan artifacts + columnar batch ABI (`wasm_batch_v1`).
- **Hybrid row+column snapshots:** OLTP-first with analytics-friendly snapshots.
- **Cluster control-plane (experimental):** `cluster.*` endpoints, join tokens, shard placement, and primary->replica write fanout.
- **SkeinAdmin control panel:** click-first workspace, inline grid row editing, optional visual row editor, inline Easy Viewer DB creation + live create-table preview/validation, identifier-safe SQL generation, fail-closed destructive ops, dialect-aware SQL profiles, settings explorer (`settings.list` + capability shortcuts), dedicated telemetry/security panels, password-backed DB-user management, persisted HTTP bearer token management, expert cluster/settings panels, and a live Index Advisor page with ranked suggestions plus observed-before/expected-after scan reports.
- **Graceful shutdown controls:** `Ctrl+C`, `SIGTERM`, or `system.shutdown` now checkpoint state and update cluster node status.

---

## Requirements

- Rust toolchain (stable) for the server, tests, and CLI.
- Node.js is only needed if you want to rebuild or serve the web assets under `web/`.

---

## Quick start

### Build

```bash
cargo build --release
```

### Run

```bash
./target/release/skeindb serve --data ./data --http 8080 --mysql 3306
```

With PostgreSQL listener (partial baseline):

```bash
./target/release/skeindb serve --data ./data --http 8080 --mysql 3306 --pg 5432
```

Optional storage mode:

```bash
./target/release/skeindb serve --data ./data --http 8080 --mysql 3306 --storage-mode hybrid
```

Default row persistence without an explicit flag is `segment`, which stores table rows in `.rseg` files and falls back to `.json` on read when needed.

Open:
- SkeinAdmin: `http://127.0.0.1:8080/admin`
- SkeinQL JSON-RPC: `http://127.0.0.1:8080/api/v1/rpc`

See `docs/GETTING_STARTED.md` for a fuller walkthrough.

---

## Key artifacts

- `./target/release/skeindb`: release build artifact produced by `cargo build --release`
- `docs/figures/architecture.png`: current architecture diagram used in docs
- `docs/site/index.html`: generated static docs landing page
- `site/index.html`: generated project landing page
- `tests/compat/corpus.sql`: MySQL compatibility regression corpus
- `samples/sample.sql`: minimal SQL sample file

---

## Docs

Start here:
- `docs/README.md` (documentation index)

Frequently used:
- `docs/SKEINQL.md`
- `docs/QUERY_PATCH.md`
- `docs/ETAG_VALIDATORS.md`
- `docs/TRAFFIC_REDUCTION.md`
- `docs/MYSQL_COMPAT.md`
- `docs/PG_COMPAT.md`
- `docs/SKEINADMIN.md`
- `docs/GETTING_STARTED.md`

---

## Repo layout

```text
crates/
  skeindb/          # server + prototype execution engine
  skeindb-core/     # stable primitives (ValueIDs, hashes, canonicalization)
  skeindb-ir/       # intermediate representation types shared across layers
  skeindb-skeinql/  # SkeinQL types + JSON-RPC method schemas
web/
  console/          # minimal embedded SQL console sources
  skeinadmin/       # embedded management UI (admin + console routes)
openapi/
  skeinql.yaml      # minimal API sketch
docs/               # specs, research notes, and operator docs
samples/            # example SQL inputs
tests/compat/       # MySQL compatibility corpus and regression inputs
site/               # generated public landing page artifact
```

---

## Verification

Run the standard repo checks from the workspace root:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```

---

## Status

This repo contains both:
- working prototype code (HTTP, SkeinQL, admin console)
- research-grade specifications and an implementation backlog

For what is implemented vs planned, see the docs and the backlog:
- `docs/PROJECT_BACKLOG.md`
- `docs/RESEARCH_AGENDA.md`
- `docs/TRUE_STATUS_MATRIX.md`

Recent documentation updates (2026-03-30):
- `docs/PG_COMPAT.md`: rewritten to the current PG v3 baseline and open tasks
- `docs/PROJECT_BACKLOG.md`: status sync for PostgreSQL docs + current corpus numbers
- `docs/TRUE_STATUS_MATRIX.md`: refreshed backlog counts and compatibility snapshot
- `site/index.html` / `docs/site/index.html`: synced public stats and feature badges

---

## License

SkeinDB is licensed under the Apache License 2.0. See `LICENSE`.
