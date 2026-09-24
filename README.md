# SkeinDB

[![CI](https://github.com/pinkysworld/SkeinDB/actions/workflows/ci.yml/badge.svg)](https://github.com/pinkysworld/SkeinDB/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

SkeinDB is a Rust relational database and systems research platform built around **content identity**. It reuses that identity to deduplicate stored values, transfer only missing objects between replicas, and validate or patch query results without resending unchanged data.

It runs as one executable and exposes a MySQL-compatible SQL surface, a partial PostgreSQL v3 surface, SkeinQL over HTTP, optional QUIC transport, and an embedded administration UI.

## What it does

- **Store less:** content-addressed values, deduplication, and delta chains.
- **Move less:** hash-verified CAS replication, including a compact binary fetch response with legacy fallback.
- **Return less:** dependency-aware ETags and QueryPatch responses.
- **Reuse work:** query coalescing, plan caching, autoparameterization, and incremental views.
- **Operate and investigate:** CDC, time travel and replay, audit proofs, encryption controls, cluster status, and SkeinAdmin workflows.

These capabilities have different maturity levels. The [true status matrix](docs/TRUE_STATUS_MATRIX.md) is the source of truth for what is implemented, partial, or still prototype-strength.

## Compatibility and limits

| Surface | Current status |
| --- | --- |
| MySQL | Broad, corpus-backed SQL compatibility; not full MySQL parity. |
| PostgreSQL | Substantial but partial PG v3 protocol and SQL support; not full PostgreSQL parity. |
| SkeinQL | Native typed JSON-RPC API over HTTP, with optional QUIC transport. |
| Storage | `.rseg` snapshots plus a per-database redo WAL protect row writes. New WAL records keep encrypted row and primary-key data encrypted, and keyless recovery retains the WAL until the key is registered. Most tables materialize in memory; opt-in streaming supports eligible large segment-backed reads. The core MANIFEST/WAL/LSM pipeline is not yet the primary row store. |
| Research features | R01–R18 and R20 have evidence-backed implementations; R19 Wasm query operators remain a prototype. |

Other remaining gaps include broader PostgreSQL driver and catalog coverage, richer CDC predicates and external sinks, deeper compaction, and signed package publication that depends on release configuration. See [docs/TRUE_STATUS_MATRIX.md](docs/TRUE_STATUS_MATRIX.md) for the complete list and scope.

Streaming reads are opt-in through `SKEINDB_STREAMING_MIN_BYTES` and apply only to eligible segment-backed tables; writes materialize those tables. `SKEINDB_WAL_SYNC_BATCH` defaults to `1` (fsync each commit). Larger batches improve write throughput but can lose up to `N-1` recent commits per database after power loss.

## Installation

On macOS, install the current development build from the project tap:

```bash
brew tap pinkysworld/skeindb https://github.com/pinkysworld/SkeinDB
brew install --HEAD pinkysworld/skeindb/skeindb
```

Tagged releases also update the stable formula. See [Getting Started](docs/GETTING_STARTED.md) for apt setup and the stable Homebrew command, and [Release Packaging](docs/RELEASE_PACKAGING.md) for package status.

After the signed apt repository has been published and configured:

```bash
apt-get install skeindb
```

## Quick start

Build with stable Rust:

```bash
cargo build --release
```

Start the server on loopback:

```bash
./target/release/skeindb serve --data ./data --http 8080 --mysql 3306 --pg 5432
```

Check the HTTP endpoint and call SkeinQL:

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/api/v1/rpc \
  -H 'content-type: application/json' \
  -d '{"skeinql":"1.0","id":"1","method":"system.ping","params":{}}'
```

Open [SkeinAdmin](http://127.0.0.1:8080/admin). Use `--help` for server options, including storage mode and listener ports. Read [Getting Started](docs/GETTING_STARTED.md) for a first schema and query walkthrough, and [Configuration](docs/CONFIGURATION.md) before exposing listeners beyond loopback. Install packages with the instructions in [Release Packaging](docs/RELEASE_PACKAGING.md).

## Evaluation evidence

The repository keeps analytical and live-runtime measurements separate:

- [Redundancy pipeline evaluation](docs/REDUNDANCY_PIPELINE_EVAL.md) explains the cross-layer byte model and its limits.
- [Default analytical report](eval/reports/redundancy_pipeline_default.md) records the reproducible workload results.
- [Live runtime report](eval/reports/redundancy_runtime_ci.md) measures storage, object transfer, and RPC response bytes.
- [CAS HTTP wire report](eval/reports/cas_http_wire_ci.md) records two-node transfer behavior and HTTP byte counts.

These byte measurements are not claims about general production latency or throughput.

## Project map

```text
crates/       engine, storage primitives, SkeinQL types, and query IR
docs/         operator guides, specifications, status, and research notes
tests/        unit, integration, compatibility, and protocol coverage
eval/         reproducible evaluation scripts and checked-in reports
samples/      runnable examples
web/          SkeinAdmin and console assets
site/         project website
```

Start with [docs/README.md](docs/README.md) for the documentation index. The [project backlog](docs/PROJECT_BACKLOG.md) and [research backlog](docs/RESEARCH_BACKLOG.md) track work; a completed checklist item does not imply production completeness.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution workflow and [SECURITY.md](SECURITY.md) for vulnerability reporting.

## License

SkeinDB is licensed under [Apache-2.0](LICENSE).
