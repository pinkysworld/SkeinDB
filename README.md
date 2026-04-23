# SkeinDB

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Sponsor](https://img.shields.io/badge/%E2%9D%A4-Sponsor-ea4aaa)](https://github.com/sponsors/pinkysworld)
[![Commercial](https://img.shields.io/badge/commercial-options-6366f1)](COMMERCIAL.md)

Last updated: 2026-04-23.

SkeinDB is a single-binary database server that combines:

- a MySQL-compatible adoption layer
- a native HTTP/JSON-RPC control plane called SkeinQL
- an embedded admin UI at `/admin` and `/console`
- an early PostgreSQL wire-protocol baseline
- a large set of research and systems experiments in the same codebase

The short version: SkeinDB already runs as a real server with a usable admin console, a broad MySQL compatibility surface, and a growing PostgreSQL baseline, but it is still honest-to-goodness work in progress rather than a finished production database.

![SkeinDB architecture](docs/figures/architecture.png)

---

## What SkeinDB Is Today

### Working now

- One executable runs the HTTP API, SkeinAdmin, the MySQL listener, and the optional PostgreSQL listener.
- MySQL compatibility is the most mature adoption path and is exercised by the checked-in compatibility corpus and end-to-end tests.
- WordPress-class MySQL workloads are a first-class target: the repo covers installer/admin query shapes and live WordPress smoke tests.
- SkeinAdmin is a real embedded control panel with schema browsing, SQL workspaces, an Easy Viewer, settings management, token/user management, observability views, and index-advisor workflows.
- SkeinQL is the preferred native API for new apps and is available over HTTP, with QUIC support in the codebase as well.
- Row persistence now defaults to segment-backed `.rseg` storage.

### Partial / still growing

- PostgreSQL support is real but still partial: startup/auth, common bootstrap probes, simple-query execution, and failed-transaction `ReadyForQuery(E)` handling work, but broader dialect, catalogs, and driver parity are still open.
- Several research tracks are implemented as usable prototypes or hardened baselines, but not all are production-grade.
- Clustering, CDC, snapshots, Wasm operators, and advisor flows exist, but some areas still need hardening and broader lifecycle support.

### Not true yet

- SkeinDB is not full MySQL parity.
- SkeinDB is not full PostgreSQL compatibility.
- The storage engine and several advanced features are still in prototype or hybrid states rather than fully hardened production implementations.

> Implementation note
> The current engine is usable and tested, but parts of the storage and research architecture are still evolving. The repo intentionally contains both shipped runtime behavior and forward-looking implementation work.

---

## Current Status

- **MySQL:** broad compatibility layer with prepared statements, wide `COM_QUERY` coverage, compatibility shims for real application workloads, and corpus-backed regression coverage.
- **WordPress:** install/admin-style compatibility is far enough along to be used as a live smoke target, including Users and Site Health query coverage.
- **PostgreSQL:** partial PG v3 baseline with trust/cleartext auth, managed DB-user passwords, SSL rejection, startup probes, simple queries, and failed-transaction blocking.
- **Admin/UI:** SkeinAdmin is no longer a placeholder; it is an active part of the product surface. Easy Viewer now ships a **WYSIWYG schema editor** (Easy Viewer → Design tab) that diffs your in-browser edits against the live table and emits a `ALTER TABLE` plan you can preview before applying.
- **Storage:** default row persistence is `segment` mode using `.rseg`, with fallback/hybrid support still present.
- **CLI:** `skeindb version` prints a runtime banner with format and dialect doc pointers; `skeindb info --data ./data [--json]` summarises catalog state, storage mode, and default ports for ops use; `skeindb serve` prints a startup banner with the resolved data dir, storage mode, and listener URLs (HTTP / SkeinAdmin / MySQL / PostgreSQL / QUIC / cluster).
- **Status tracking:** the authoritative runtime truth lives in `docs/TRUE_STATUS_MATRIX.md`, with the roadmap in `docs/PROJECT_BACKLOG.md`.

If you want the most honest snapshot of what is implemented versus planned, start here:

- `docs/TRUE_STATUS_MATRIX.md`
- `docs/PROJECT_BACKLOG.md`
- `docs/MYSQL_COMPAT.md`
- `docs/PG_COMPAT.md`

---

## Why Use It

SkeinDB is useful if you want one of these:

- a single local binary for SQL experiments, admin tooling, and protocol testing
- a MySQL-compatible target for adoption and migration work
- a controllable environment for research features like ETags, query patches, audit logs, vector search, and Wasm execution
- a codebase that keeps runtime features, backlog, and docs close together instead of hiding the gap

---

## Quick Start

### Install

Homebrew:

```bash
brew tap pinkysworld/skeindb https://github.com/pinkysworld/SkeinDB
brew install --HEAD pinkysworld/skeindb/skeindb
```

Tagged `v*` releases update the repo-local Homebrew formula automatically, after which the stable path is:

```bash
brew install pinkysworld/skeindb/skeindb
```

apt-get:

```bash
sudo curl -fsSL https://raw.githubusercontent.com/pinkysworld/SkeinDB/apt/pubkey.gpg \
  -o /usr/share/keyrings/skeindb-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/skeindb-archive-keyring.gpg] https://raw.githubusercontent.com/pinkysworld/SkeinDB/apt stable main" \
  | sudo tee /etc/apt/sources.list.d/skeindb.list >/dev/null
sudo apt-get update
sudo apt-get install skeindb
```

The apt repository is published by the tag-driven release workflow once the signing secrets are configured.

### Build

```bash
cargo build --release
```

### Run

```bash
./target/release/skeindb serve --data ./data --http 8080 --mysql 3306
```

With PostgreSQL enabled:

```bash
./target/release/skeindb serve --data ./data --http 8080 --mysql 3306 --pg 5432
```

Optional storage mode override:

```bash
./target/release/skeindb serve --data ./data --http 8080 --mysql 3306 --storage-mode hybrid
```

Default row persistence without an explicit flag is `segment`, which stores table rows in `.rseg` files and falls back to `.json` on read when needed.

Open:

- SkeinAdmin: `http://127.0.0.1:8080/admin`
- SQL workspace: `http://127.0.0.1:8080/console`
- SkeinQL JSON-RPC: `http://127.0.0.1:8080/api/v1/rpc`

See `docs/GETTING_STARTED.md` for a fuller walkthrough.

---

## Main Surfaces

### MySQL

- MySQL wire listener on `--mysql`
- `mysql_native_password` handshake/auth flow
- broad translated SQL subset
- prepared-statement support
- compatibility coverage aimed at real application workloads, especially WordPress-shaped traffic

See `docs/MYSQL_COMPAT.md`.

### PostgreSQL

- PG v3 startup/auth handshake
- common startup/bootstrap query handling
- simple query protocol
- failed transaction state in the simple-query path

See `docs/PG_COMPAT.md`.

### SkeinQL

- JSON-RPC control plane over HTTP
- schema, query, transaction, admin, telemetry, cluster, and research-oriented surfaces

See `docs/SKEINQL.md`.

### SkeinAdmin

- embedded admin and console routes
- schema/data/sql workflows
- Easy Viewer for click-first table work
- settings, telemetry, security, and advisor panels

See `docs/SKEINADMIN.md`.

---

## Repository Layout

```text
crates/
  skeindb/          # server, protocol layers, execution engine
  skeindb-core/     # stable low-level primitives
  skeindb-ir/       # shared IR types
  skeindb-skeinql/  # SkeinQL request/response and method schemas
web/
  console/          # minimal embedded SQL console sources
  skeinadmin/       # embedded admin UI sources
docs/               # operator docs, specs, compatibility notes, backlog
tests/compat/       # MySQL compatibility corpus and regressions
site/               # generated public landing page
```

---

## Verification

Standard checks from the workspace root:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```

Note: strict `clippy -D warnings` is still not clean repo-wide today; use `docs/TRUE_STATUS_MATRIX.md` and current CI/local output as the source of truth for that status.

## Release Packaging

Tagged releases now drive the install surfaces:

- `vX.Y.Z` tags build a source tarball, a Linux `amd64` tarball, and a Debian package.
- The same workflow renders a stable `Formula/skeindb.rb` entry in this repo for the Homebrew tap.
- If `APT_GPG_PRIVATE_KEY`, `APT_GPG_KEY_ID`, and the optional `APT_GPG_PASSPHRASE` GitHub Actions secrets are configured, the workflow also publishes a signed apt repository to the `apt` branch.

---

## Documentation

Start here:

- `docs/README.md`

Most useful day-to-day docs:

- `docs/GETTING_STARTED.md`
- `docs/MYSQL_COMPAT.md`
- `docs/PG_COMPAT.md`
- `docs/SKEINQL.md`
- `docs/SKEINADMIN.md`
- `docs/ON_DISK_FORMAT.md`
- `docs/TRUE_STATUS_MATRIX.md`
- `docs/PROJECT_BACKLOG.md`

---

## Support

If SkeinDB is useful to you and you want to help keep it moving:

- GitHub Sponsors is the main option: <https://github.com/sponsors/pinkysworld>
- If PayPal is easier, you can use `mip@gmx.biz` (or <https://www.paypal.com/paypalme/mippinky>).

For teams running SkeinDB in production we publish indicative support plans
(Starter €299 / Business €1,200 / Enterprise €3,900) plus custom and 24×7
engagement options. See the full tier table, add-ons, and FAQ on
[site/pricing.html](site/pricing.html), the contact form on
[site/contact.html](site/contact.html), or the long-form overview in
[COMMERCIAL.md](COMMERCIAL.md).

See [SUPPORT.md](SUPPORT.md) for a shorter community-support overview.

---

## License

SkeinDB is licensed under the Apache License 2.0. See `LICENSE`.
