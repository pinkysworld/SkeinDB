# Configuration

SkeinDB is designed to run as a **single executable** with configuration primarily via CLI flags.

The goal is a low-friction deployment model:
- copy the binary
- pick ports
- pick a data directory
- run

> The exact flags may evolve; this document describes the intended interface and the current scaffold behavior.

---

## CLI

```text
skeindb serve [OPTIONS]

OPTIONS:
  --data <path>      Data directory (WAL, snapshots, metadata)
  --storage-mode     Table row persistence mode: json | segment | hybrid (default segment)
  --http <port>      HTTP port (SkeinQL + admin console)
  --mysql <port>     MySQL protocol port (compatibility surface)
  --pg <port>        PostgreSQL protocol port (partial v3 baseline, default 5432; 0 = disabled)
  --bind <ip>        Bind address (default 127.0.0.1)
```

### Examples

Run on ports 8080/3306:

```bash
./skeindb serve --data ./data --http 8080 --mysql 3306
```

Run with MySQL + PostgreSQL:

```bash
./skeindb serve --data ./data --http 8080 --mysql 3306 --pg 5432
```

Run with JSON-only row files:

```bash
./skeindb serve --data ./data --http 8080 --mysql 3306 --storage-mode json
```

Run HTTP-only:

```bash
./skeindb serve --data ./data --http 8080 --mysql 0
```

---

## Environment variables

`SKEINDB_STORAGE_MODE` controls how per-table row files are persisted:
- `json`: read/write `tables/<db>/<table>.json`
- `segment`: read/write `tables/<db>/<table>.rseg`
- `hybrid` / `dual`: write both files; read prefers `.rseg` then `.json`
- default `serve` mode: `segment`

Example:

```bash
SKEINDB_STORAGE_MODE=segment ./skeindb serve --data ./data --http 8080 --mysql 3306
```

Notes:
- CLI `--storage-mode` takes precedence for `serve`.
- Environment variable is still used by non-serve workflows that open the engine directly.

---

## HTTP services

When enabled, the HTTP listener serves:
- `POST /api/v1/rpc` SkeinQL JSON-RPC
- `GET /api/v1/q/:query_id` prepared query execution (ETag validators)
- `GET /admin` (SkeinAdmin)
- `GET /metrics` (Prometheus-style counters)

## MySQL listener

When `--mysql` is non-zero, SkeinDB also starts a MySQL protocol listener.
Current coverage:
- connection handshake
- `mysql_native_password` auth exchange
- `COM_QUERY` SQL translation subset (`SELECT/SHOW/USE/CREATE DATABASE/CREATE TABLE/DROP TABLE/INSERT/UPDATE/DELETE`)
- `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` / `COM_STMT_CLOSE` (prepared statements)
- 678 semicolon-terminated compatibility SQL statements in `tests/compat/corpus.sql`

Additional SQL compatibility remains corpus-driven and is tracked in the compatibility docs.

---

## PostgreSQL listener (partial baseline)

When `--pg` is non-zero, SkeinDB starts a PostgreSQL v3 wire protocol listener.
Current coverage:
- StartupMessage + SSLRequest parsing
- trust authentication when `SKEINDB_TOKEN` is unset
- SCRAM-SHA-256 authentication when `SKEINDB_TOKEN` is set
- SSL negotiation rejection (`'N'`)
- startup `ParameterStatus` / `BackendKeyData` / `ReadyForQuery` sequence
- simple query protocol delegated to the shared SQL engine
- transaction/savepoint state with PostgreSQL-style `ReadyForQuery` statuses
- extended-query protocol for `Parse` / `Bind` / `Describe` / `Execute` / `Sync` / `Close` / `Flush`

Configuration in `skeindb-config.json`:
```json
{
  "pg_port": 5432
}
```

See `docs/PG_COMPAT.md` for the current scope, tests, and remaining gaps.

---

## Data directory layout (prototype)

The in-memory prototype persists a small amount of metadata.

Planned layout (subject to change as the WAL/segment formats are implemented):

```text
<data>/
  wal/
  snapshots/
  meta/
```

---

## Running behind a reverse proxy

SkeinDB is compatible with standard reverse proxies (Apache, Nginx, IIS) because the control plane is HTTP.

Recommended settings:
- keep HTTP/2 enabled where possible
- use TLS termination at the proxy
- enable gzip/brotli compression
- set cache headers for prepared-query GET endpoints (`/api/v1/q/...`)

---

## Clustering settings

Clustering is managed via SkeinQL (`cluster.*`) and is designed to be configured without external orchestration.

See:
- `docs/CLUSTERING.md`
