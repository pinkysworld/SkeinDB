# SkeinDB

[![CI](https://github.com/pinkysworld/SkeinDB/actions/workflows/ci.yml/badge.svg)](https://github.com/pinkysworld/SkeinDB/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Sponsor](https://img.shields.io/badge/%E2%9D%A4-Sponsor-ea4aaa)](https://github.com/sponsors/pinkysworld)
[![Commercial](https://img.shields.io/badge/commercial-options-6366f1)](COMMERCIAL.md)

Last updated: 2026-09-22.

**SkeinDB is a redundancy-aware relational database that carries content identity from storage to replication to query delivery.**

The central idea is simple: databases repeatedly store, move, compare, and return values that are identical or only slightly changed. SkeinDB makes value identity a first-class primitive and reuses it across the stack.

```text
                one logical value identity
                         │
        ┌────────────────┼────────────────┐
        │                │                │
     storage         replication      query delivery
        │                │                │
  ValueID/CAS       missing-object     ETags +
  dedup + delta      transfer only      QueryPatch
        │                │                │
   fewer bytes       fewer bytes       fewer bytes
    on disk          on the wire       per response
```

That gives SkeinDB a coherent systems thesis rather than a collection of isolated features:

- **Store less:** content-addressed values deduplicate repeated cells across rows, tables, and retained MVCC versions; delta chains compact similar payloads.
- **Move less:** CAS-aware replication transfers only missing ValueIDs and verifies fetched objects by hash.
- **Return less:** dependency-derived ETags, conditional reads, and query-scoped delta responses avoid retransmitting unchanged result state.
- **Recompute less:** plan caching, autoparameterization, query coalescing, incremental views, and dependency tracking reuse prior work where correctness allows it.

A deterministic cross-layer evaluation now exercises this thesis on one workload and reports storage, CAS object-transfer, and QueryPatch byte savings separately. See [docs/REDUNDANCY_PIPELINE_EVAL.md](docs/REDUNDANCY_PIPELINE_EVAL.md) and the checked-in [default report](eval/reports/redundancy_pipeline_default.md). The report is explicitly an analytical byte model, not a live latency or throughput benchmark.

That model is now paired with a **live-engine validation** that reads runtime storage counters, exercises real ValueStore object-transfer counters, and measures actual RPC response bytes. The current runtime snapshot is in [eval/reports/redundancy_runtime_ci.md](eval/reports/redundancy_runtime_ci.md).

CAS transfer is also validated **across two independent live SkeinDB processes**: the destination is pre-seeded with a controlled subset of real ValueIDs, then pulls the missing objects from the source through the production `objects.pull -> objects.fetch` HTTP path. The checked-in two-node evidence is [eval/reports/cas_two_node_ci.md](eval/reports/cas_two_node_ci.md), including an idempotence check that a second pull performs zero remote fetches.

The same path is measured at the **HTTP-over-TCP byte level** through a transparent proxy. CR05 now makes replication pulls request a compact, backward-compatible `transfer_only` fetch representation. Against the preserved pre-CR05 snapshot, zero-overlap HTTP transfer drops by about **60%**, and large-transfer wire/object amplification falls from ~**2.55x** to ~**1.00-1.01x**, while a repeated fully synchronized pull still transfers **0 bytes**. See [post-CR05 evidence](eval/reports/cas_http_wire_ci.md) and the [preserved pre-CR05 baseline](eval/reports/cas_http_wire_pre_cr05.md).

CR06 also reuses a single HTTP client across all batches in one pull. The same wire harness now verifies **one TCP connection per non-empty pull** instead of one connection per batch: 8->1 and 6->1 in the largest standard scenario, and 5->1 / 2->1 in the balanced case. The application-byte totals are intentionally unchanged. The [pre-CR06 snapshot](eval/reports/cas_http_wire_pre_cr06.md) is retained for comparison.

CR07 then sweeps batch sizes 16/32/64/128/256 on that same live wire path. The existing default **64** is retained: it saves 3.11% combined HTTP bytes versus 32, while 128 and 256 provide only about 1.29% and 2.26% additional savings versus 64 at the cost of larger retry units. See the [batch-sweep evidence](eval/reports/cas_batch_sweep_ci.md).

CR08 decomposes the remaining wire bytes. About **54.6-54.9%** are Base64 transfer-entry JSON, about **16%** are request ValueIDs, and another **16%** are repeated response ValueIDs; HTTP headers are only about 2%. This evidence makes a backward-compatible **binary replication fetch** the next transport target. See the [wire-composition evidence](eval/reports/wire_composition_ci.md).

CR09 implements that binary response while keeping a JSON request and automatic legacy fallback. At batch 32, zero-overlap wire bytes fall **56,564 -> 34,361 B**, **30,280 -> 18,426 B**, and **6,677 -> 4,059 B** versus the preserved pre-CR09 path, about **39% lower**. At the validated batch size 64, the low-redundancy CAS transfer is **24,806 B** and the balanced transfer **7,061 B**. The remaining wire is now dominated by canonical transfer-entry bytes (~66%) and request-side hexadecimal ValueIDs (~27%), making raw 16-byte request IDs the next measured transport target. See the [post-CR09 wire report](eval/reports/cas_http_wire_ci.md), [pre-CR09 baseline](eval/reports/cas_http_wire_pre_cr09.md), and [composition report](eval/reports/wire_composition_ci.md).

SkeinDB is also deliberately usable as software, not only as a research prototype. One executable can expose MySQL, PostgreSQL v3, SkeinQL over HTTP/JSON-RPC, optional QUIC, and the embedded SkeinAdmin console.

## Project identity

SkeinDB has two layers:

1. **SkeinDB the database:** a Rust database engine with SQL compatibility, durable storage, MVCC, time travel, CDC, replication, HA controls, observability, backup/restore, and an embedded admin UI.
2. **SkeinDB the research platform:** a set of focused research tracks that use the same engine to evaluate ideas in content-addressed storage, replay, privacy, adaptive execution, Wasm, indexing, transport, schema evolution, and energy-aware maintenance.

The database is intentionally broader than any single paper. Research claims should therefore be evaluated track by track, with the strongest unifying thread being **redundancy-aware database architecture**.

## Truth snapshot

- **Compatibility:** MySQL is the broadest adoption surface; PostgreSQL is a substantial but still partial PG v3 implementation. SkeinDB does **not** claim 100% MySQL or PostgreSQL compatibility.
- **Core roadmap:** all 140 top-level core checklist items are closed, while some phases still contain production-hardening work.
- **Research roadmap:** R01-R18 and R20 are marked hardened; R19 Wasm query operators remains prototype-strength.
- **Status authority:** [docs/TRUE_STATUS_MATRIX.md](docs/TRUE_STATUS_MATRIX.md) is the short source of truth for implemented, partial, and prototype areas. [docs/PROJECT_BACKLOG.md](docs/PROJECT_BACKLOG.md) and [docs/RESEARCH_BACKLOG.md](docs/RESEARCH_BACKLOG.md) contain the detailed task history.

## Core architecture

The features that matter most to the architecture are already wired into the runtime:

- **Content-addressed ValueStore.** Values are hash-addressed and reused instead of blindly duplicated. Runtime metrics expose dedup ratio and bytes saved.
- **Delta-chained values.** Similar payloads can be represented relative to a base value, with bounded chain depth and rebase during compaction.
- **MVCC + time travel.** Historical reads can target retained timestamps, with configurable retention and garbage collection.
- **Dependency-aware caching.** Prepared queries track dependencies so ETags and invalidations follow the data a result actually depends on.
- **QueryPatch delivery.** Changed result state can be represented as a scoped delta rather than forcing a full payload retransmission.
- **CAS-aware replication.** Replicas fetch only content objects they do not already have.
- **Durable replay.** Replay bundles capture schema, retained row versions, change metadata, checksums, and optional performance state for reproducible analysis.
- **Tamper-evident forensics.** Audit records are hash chained and can be checked or exported with proof material.

Around that core, SkeinDB also ships vector search, differential privacy, oblivious-execution controls, Wasm extensions, incremental materialized views, changefeeds, index advising, migration analysis, and an embedded operations console. Those are useful capabilities, but they are not the one-sentence definition of the project.

## Adoption surfaces

| Surface | Current role |
|---|---|
| MySQL wire protocol | Broadest compatibility path and WordPress-class workload target |
| PostgreSQL v3 | Partial but substantial compatibility path |
| SkeinQL | Native typed JSON-RPC control/query surface |
| HTTP query endpoints | Cache-coherent and conditional delivery surface |
| QUIC | Optional native transport and research surface |
| SkeinAdmin | Embedded administration, observability, replay, privacy, CDC, and schema workflows |

The compatibility suite includes a **1600+ line MySQL corpus**, live WordPress-oriented coverage, PostgreSQL roundtrips, crash-recovery tests, and focused research-track tests.

![SkeinDB architecture](docs/figures/architecture.png)

---

## What's Working Today

- **One executable** runs the HTTP API, SkeinAdmin, the MySQL listener, and the optional PostgreSQL listener — no sidecars, no proxy, no separate console process.
- **MySQL compatibility** is the most mature adoption path. The 1600-line compatibility corpus covers DML, joins, aggregates, window functions, JSON functions, CTEs, UNION, GROUP BY, prepared statements, and more — and runs end-to-end on every commit.
- **WordPress-class workloads** are a first-class target: installer/admin query shapes are covered, and a live WordPress smoke test runs against the listener.
- **PostgreSQL v3** wire baseline with SCRAM-SHA-256 auth, simple + extended query protocol, virtual `pg_catalog` including table/view, role/user, index, tablespace, sequence/statistics, and database-stat probes, transaction/savepoint state, and SQLSTATE-mapped errors.
- **SkeinAdmin** is a real embedded control panel: schema browsing, SQL workspaces, Easy Viewer with inline edit + WYSIWYG schema design, dashboards with live storage/dedup/MVCC/cache cards, settings + token/user management, telemetry, privacy controls, index-advisor workflows, CDC, time-travel, replay, encryption, and forensic query/proof export workflows.
- **SkeinQL** is the preferred native API: typed JSON-RPC over HTTP and QUIC.
- **Row persistence** defaults to segment-backed `.rseg` storage.
- **Durable, crash-safe storage.** All on-disk writes go through an atomic temp→`fsync`→rename→dir-`fsync` path, backed by a row-level redo **write-ahead log** with idempotent crash recovery (validated by a torn-tail fault-injection test that truncates the WAL at every offset). Snapshot flushes are **deferred and batched** so a mutation doesn't rewrite the whole table on every commit, and an opt-in **WAL group commit** (`SKEINDB_WAL_SYNC_BATCH`) amortizes the fsync under concurrent write load. The CDC change-log and forensic hash-chain are **append-only** (one length-prefixed record per mutation, compacted at each flush) instead of a full rewrite + fsync per mutation — a ~7x write-throughput improvement that keeps every crash-durability and tamper-evidence guarantee. A corrupt table file loads empty and refuses to be overwritten.
- **Query-time streaming for tables larger than RAM** (opt-in `SKEINDB_STREAMING_MIN_BYTES`). Eligible large tables are read directly off their on-disk segment without materializing, with a seek-based on-disk primary-key index for point lookups; writes materialize on demand.
- **Operational readiness.** Cooperative **statement timeout** (`SKEINDB_STATEMENT_TIMEOUT_MS`) aborts runaway queries; `/metrics` exposes per-method query latency/error/quantile and storage-engine internals in Prometheus format, with an opt-in **slow-query log** (`SKEINDB_SLOW_QUERY_MS`); `skeindb backup` / `skeindb restore` make and verify crash-consistent copies; internal lock poisoning is non-fatal; and startup warns if the API is bound to a non-loopback address without a token.
- **Opt-in RBAC on the RPC path** (`SKEINDB_RBAC`). Beyond the single shared bearer token, per-role authorization can be enabled so each request resolves to a principal — the `SKEINDB_TOKEN` superuser, an API-token secret (with an optional per-database `db_scope`), or a database user's login secret — and each method is checked against a `read < write < admin` privilege before dispatch; denied calls return `403`. Granularity is **role → database → table**: a user's grants can target a whole database or a specific `db.table`, and database provisioning (`create/drop database`) requires `admin`. Off by default (legacy single-token behavior unchanged). See [docs/CONFIGURATION.md](docs/CONFIGURATION.md#rbac-role-based-access-control-on-the-rpc-path).
- **High availability with automated fenced failover** (opt-in `SKEINDB_CLUSTER_AUTO_FAILOVER`). Nodes heartbeat each other; a primary that loses quorum **fences itself** (refuses writes) and the majority side elects a new primary through a **Raft-style vote round** (a candidate promotes only with a majority of per-term votes), with a monotonic leadership epoch as the fencing token. Two disjoint partitions can never both hold a quorum, so at most one primary accepts writes. Sharded clusters fail over **per shard** — each shard is its own replication group with an independent quorum, epoch, and election. Off by default (failover stays manual + quorum-gated). See [docs/CONFIGURATION.md](docs/CONFIGURATION.md#failure-detection--failover-readiness).
- **Data-safe failover on true log positions.** Every replicated write carries a primary-assigned log position `(term, index)`, and both candidate selection and the vote round compare that position (not a heuristic count): a later term outranks an earlier one, a higher sequence wins within a term, and a voter **refuses any candidate less caught up than itself**. With the majority-vote rule, the elected primary provably holds every committed write — automatic failover cannot lose acknowledged data.
- **Self-healing replication + commit index.** A replica that falls behind on a transient blip, or joins late, **catches up automatically** — the primary keeps a bounded op-log and the replica pulls the ops it missed (`cluster.replication.fetch`), applying them idempotently and in order. The primary computes and propagates a **commit index** (the log position a majority has durably replicated); `cluster.replication.status` reports it on every node and `cluster.failover.status` reports each node's `commit_lag`, so you can see exactly which replicas are behind on durability. See [docs/CLUSTERING.md](docs/CLUSTERING.md) §2.5.

## What's Still Partial

- PostgreSQL support is real but still partial: COPY protocol, portal suspension, broader dialect/catalog parity, and production-grade driver matrices are still open. See [docs/PG_COMPAT.md](docs/PG_COMPAT.md).
- Nineteen research tracks (`R01`-`R18` and `R20`) are hardened with evidence-backed tests; `R19` Wasm query operators remains prototype implemented. See [docs/TRUE_STATUS_MATRIX.md](docs/TRUE_STATUS_MATRIX.md).
- Clustering, CDC, snapshots, Wasm operators, and advisor flows are wired end-to-end; CDC still needs broader predicates, alternative event encodings, external sinks, and cluster-wide fanout, while R19 still does not claim production SIMD-lowered codegen.
- The HA/consensus path includes commit-index-aware read-committed reads and automated bounded-memory snapshot re-sync. Remaining distribution work is mainly broader hardening, availability/performance work, and edge-case coverage rather than those earlier correctness gaps. See [docs/CLUSTERING.md](docs/CLUSTERING.md) §2.5 and [docs/PERFORMANCE.md](docs/PERFORMANCE.md).
- SkeinDB does **not** claim 100% MySQL or PostgreSQL parity.

> Implementation note
> The current engine is usable and tested, but parts of the storage and research architecture are still evolving. The repo intentionally keeps shipped runtime behavior and forward-looking work next to each other so the gap is always visible.

---

## Current Status

- **MySQL:** broad compatibility layer with prepared statements, wide `COM_QUERY` coverage, compatibility shims for real application workloads, and corpus-backed regression coverage.
- **WordPress:** install/admin-style compatibility is far enough along to be used as a live smoke target, including Users and Site Health query coverage.
- **PostgreSQL:** partial PG v3 baseline with trust/SCRAM-SHA-256 auth, managed DB-user passwords, SSL rejection, startup probes, simple + extended query protocol, virtual `pg_catalog` including table/view, role/user, index, tablespace, sequence/statistics, and database-stat probes, SQLSTATE-mapped errors, and failed-transaction blocking.
- **Merge/CRDT:** R07 is hardened with `merge.apply`, `merge.simulate`, `merge.evaluate`, values-only Wasm merge execution, fuel/time cancellation coverage, offline queue docs, and a SkeinAdmin Merge & CRDT panel wired to the typed runtime payloads.
- **Views:** R08 is closed with `view.create/drop/refresh/evaluate/status/explain_deps`, deterministic incremental-vs-full oracle reports, benchmark timings, MySQL/PG view catalog rows, and a SkeinAdmin Views panel for refresh mode, evaluation, status, and dependencies.
- **CDC:** Phase 23 table/query subscriptions support polling, SSE, WebSocket replay, durable cursors, pause/resume, backpressure, resnapshot signaling, row images, source-op filters, exact primary-key filters, inclusive single-column primary-key ranges, changed-column filters, and prepared-query invalidation over direct base tables, view-expanded base tables, set-operation branches, and CTE definitions.
- **Admin/UI:** SkeinAdmin is no longer a placeholder; it is an active part of the product surface. Easy Viewer now ships a **WYSIWYG schema editor** (Easy Viewer → Design tab) that diffs your in-browser edits against the live table and emits a `ALTER TABLE` plan you can preview before applying.
- **Encryption:** dedup-preserving encryption baseline (Phase 20) is shipped — `EncryptedValueStore` provides `put_encrypted` / `get_decrypted` / `reencrypt_value` over the existing storage format, `DatabaseKeyManager::rotate_active_key` returns a `KeyRotationPlan`, and `settings.encryption.*` JSON-RPC + a SkeinAdmin **Encryption** panel expose the operator surface (master keys live only in process memory; re-register on restart).
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

Pick SkeinDB if you want any of these:

- **One binary, no setup tax.** Drop it on a box, run `serve`, and you've got MySQL + PostgreSQL + JSON-RPC + admin UI. No package matrix, no separate dashboard service.
- **Storage features built in.** Dedup, delta chaining, MVCC, time travel, audit WAL, dedup-preserving encryption, and vector search are all in the same binary — toggleable from a UI checkbox, not a 200-line YAML file.
- **An admin console you'll actually open.** Easy Viewer, WYSIWYG schema editor, live dashboards, click-first CDC and replay flows. No phpMyAdmin install, no Grafana wiring.
- **Honest engineering.** The repo keeps runtime, backlog, and docs in lockstep. `docs/TRUE_STATUS_MATRIX.md` shows you what's hardened vs. prototype. We don't ship marketing claims the tests don't back.
- **A MySQL adoption target** with a corpus-backed compatibility surface and live WordPress smoke coverage.
- **A research-friendly base.** The same runtime exposes content identity, ETags, QueryPatch, replication, replay, plan/cache behavior, privacy controls, Wasm, and maintenance telemetry as measurable experimental surfaces.

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
- See `docs/RELEASE_PACKAGING.md` for the optional apt-signing behavior and why the checked-in formula can lag until the tag workflow completes.

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
