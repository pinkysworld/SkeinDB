# SkeinDB

Last updated: 2026-01-31

SkeinDB is a **single-executable** database engine scaffold that targets two goals at once:

1) **Adoption:** drop-in **MySQL compatibility** so existing applications can connect with minimal change.
2) **Research extensibility:** a clean, web-native control plane (**SkeinQL**) and a set of novel primitives (ETag-driven cache coherency, query-scoped patches, delta-chained MVCC, hash-chained WAL, sandboxed Wasm extensions) intended to make systems research easier to prototype and evaluate.

The repository is written so you can:
- run SkeinDB as a portable binary (HTTP + admin console)
- use MySQL tools as soon as the compatibility layer is implemented/expanded
- extend the engine via well-scoped crates and specs

> Implementation note
> The current execution engine is a small in-memory/JSON-backed prototype meant to make the APIs usable today.
> The paper-aligned ValueID/MVCC/LSM engine is represented as specs + backlog tasks and can be incrementally implemented.

---

## Highlights

- **Single-binary deployment:** copy one executable; pick ports; run.
- **MySQL adoption layer:** MySQL protocol surface + migration/telemetry tooling.
- **SkeinQL (native API):** JSON-RPC control plane for modern apps.
- **Web-native consistency:** ETags + If-None-Match as first-class query validators.
- **Traffic reduction:** `query.patch` deltas, patch caching/coalescing, dictionary encoding (`skeinpack_v1`).
- **MVCC extensions:** delta-chained value versions.
- **Security extensions:** hash-chained WAL for tamper evidence.
- **Sandboxed compute:** Wasm UDFs with capability-based access.
- **Wasm operators (experimental):** plan artifacts + columnar batch ABI (`wasm_batch_v1`).
- **Hybrid row+column snapshots:** OLTP-first with analytics-friendly snapshots.

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

Open:
- SkeinAdmin: `http://127.0.0.1:8080/admin`
- SkeinQL JSON-RPC: `http://127.0.0.1:8080/api/v1/rpc`

See `docs/GETTING_STARTED.md` for a fuller walkthrough.

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

---

## Repo layout

```text
crates/
  skeindb/          # server + prototype execution engine
  skeindb-core/     # stable primitives (ValueIDs, hashes, canonicalization)
  skeindb-skeinql/  # SkeinQL types + JSON-RPC method schemas
web/
  admin/            # embedded management UI (static)
openapi/
  skeinql.yaml      # minimal API sketch
docs/               # specs, research notes, and operator docs
```

---

## Status

This repo contains both:
- working prototype code (HTTP, SkeinQL, admin console)
- research-grade specifications and an implementation backlog

For what is implemented vs planned, see the docs and the backlog:
- `docs/PROJECT_BACKLOG.md`

---

## License

TBD (choose MIT/Apache-2.0 later).
