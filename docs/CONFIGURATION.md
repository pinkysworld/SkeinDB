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

### Operational tuning (all opt-in; unset = previous behavior)

| Variable | Default | Effect |
|---|---|---|
| `SKEINDB_STATEMENT_TIMEOUT_MS` | `0` (disabled) | Cooperative statement timeout. A query running longer is aborted at the next executor deadline check with a `statement_timeout` error, so a runaway query cannot hold the engine lock indefinitely. Recommended in production. |
| `SKEINDB_SLOW_QUERY_MS` | `0` (disabled) | Log any completed RPC/query at or above this duration at WARN (method, duration, rows, status, fingerprint). |
| `SKEINDB_WAL_SYNC_BATCH` | `1` | WAL group commit: `fsync` the write-ahead log once every N committed transactions instead of every one. `1` = fsync every commit (strongest durability). A larger value amortizes the fsync-under-lock cost; it can lose at most `N-1` most-recent commits on a *power-loss* crash (a clean restart still recovers everything), and recovery always yields a consistent committed prefix. |
| `SKEINDB_STREAMING_MIN_BYTES` | `0` (disabled) | Query-time streaming: a segment-backed table whose on-disk file is at least this many bytes, has a primary key, and uses no embedding/oblivious features loads as a *streaming* table — its rows stay on disk and are read on demand (with a seek-based pk index for point lookups) instead of being materialized into memory. Writes materialize the table on demand. |
| `SKEINDB_TOKEN` | unset | Bearer token for the HTTP RPC/admin API (and enables PostgreSQL SCRAM auth). **Unset = no HTTP RPC authentication.** Binding to a non-loopback address without it exposes full RPC/admin access to the network; startup logs a WARNING in that configuration. |
| `SKEINDB_RBAC` | `0` (disabled) | Opt-in per-role authorization on the HTTP RPC data path. When enabled, every RPC must present a valid credential and its method is checked against the caller's role (see [RBAC](#rbac-role-based-access-control-on-the-rpc-path) below). When disabled, the legacy single-shared-`SKEINDB_TOKEN` behavior is unchanged. |
| `SKEINDB_CLUSTER_NODE_TIMEOUT_MS` | `15000` | Cluster failure detection: a node is treated as *offline* once its last heartbeat (`cluster.node.heartbeat`) is older than this. Feeds the derived node health and the failover-candidate recommendation reported by `cluster.status` / `cluster.failover.status`. |

---

## RBAC (role-based access control) on the RPC path

By default the HTTP RPC/admin API uses a single shared bearer token (`SKEINDB_TOKEN`): a caller either has full access or none. Setting `SKEINDB_RBAC=1` (also accepts `true`/`on`) turns on per-role authorization so you can hand out least-privilege credentials.

**Principals.** With RBAC on, each request is resolved to a principal from its `Authorization: Bearer <secret>` header:

- The `SKEINDB_TOKEN` value → **superuser** (all privileges). This is the bootstrap admin credential; set it when enabling RBAC. If it is unset, only API tokens can authenticate and startup logs a warning.
- An **API-token secret** (created via `security.token.create` with a `role`) → the privilege its role confers. Secrets are compared in constant time and expired tokens are rejected.
- No/unknown credential → `401 unauthorized`.

**Roles → privileges.** Privileges are ordered `read < write < admin` (a higher level implies the lower ones):

| Role | Privilege | Can do |
|---|---|---|
| `admin` / `superuser` | admin | Everything, including user/token/cluster/encryption/settings control-plane |
| `readwrite` | write | Reads **and** data mutations + schema DDL |
| `readonly` | read | Read-only methods only |

Unknown or empty role strings fail safe to `read` (least privilege). Tokens created without an explicit role default to `admin`, so tokens issued before enabling RBAC keep full access.

**Method classification.** Read-only methods (e.g. `query.select`, `data.get`, `schema.list_tables`, `stats.*`) require `read`. Control-plane methods (`admin.user.*`, `security.token.*`, `cluster.*` mutations, `settings.set`, `settings.encryption.*`, `system.shutdown`, `maintenance.*.set_policy`, `maintenance.history.gc`, …) require `admin` — including the *listing* ones (`admin.user.list`, `security.token.list`), since they expose security configuration. Everything else — data mutations and schema DDL — requires `write`. `sql.exec` is classified by whether the statement is read-only. Unknown methods fail safe to `write` (a `readonly` principal is denied). A denied call returns `403 forbidden` and is logged at WARN.

**Database scope.** Beyond the role, an API token can be restricted to specific databases. Pass `db_scope` to `security.token.create`:

```json
{"skeinql":"1.0","id":1,"method":"security.token.create",
 "params":{"role":"readwrite","label":"analytics-app","db_scope":["analytics"]}}
```

A **database-scoped** token may only perform data-plane operations on the databases in its scope. For each request the target database(s) are extracted and every one must be in scope, otherwise the call is denied with `403 forbidden`:

- **Reads/writes on a single database** — `data.get/insert/update/delete`, `schema.list_tables/describe_table/create_table/drop_table/create_database/drop_database`, `vector.insert/search/index.status`, `view.create/drop/refresh`, `merge.apply`, and write-shaped `sql.exec` — checked against the referenced database.
- **`query.select`** — every database its `FROM` list touches (across joins, subqueries, set-operations, and CTE bodies) must be in scope; a cross-database query to an out-of-scope database is denied, naming that database.
- **Scope-neutral methods** — `system.ping/version/capabilities`, `transport.capabilities`, and `tx.begin/commit/rollback` — are always allowed for a scoped token.
- **Everything else** — global aggregates (`stats.*`), control-plane, and the prepared/patch/subscribe query variants — is **denied** for a scoped token (fail closed). Use an unscoped credential for those.

A token with no `db_scope` (the default, and every token created before this field existed) is unrestricted across databases. The role check always applies first: a `readonly` scoped token still cannot write, even within its own database.

**Scope (current slice).** Role-based enforcement plus per-token database scope (above). The username/secret `DbUser` login and its per-database `admin.user.grant` list are stored but not yet consulted on the RPC path; a user-credential login, per-table scoping, and a stricter DDL/admin split are follow-ons.

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
- `caching_sha2_password` auth exchange (advertised default; modern driver default) with `mysql_native_password` accepted as fallback
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

### Failure detection & failover readiness

Each node's liveness is tracked by the age of its last heartbeat. A node refreshes its own heartbeat by calling `cluster.node.heartbeat` (`{"node_id": "..."}`) on the primary; a node whose last heartbeat is older than `SKEINDB_CLUSTER_NODE_TIMEOUT_MS` (default 15s) is treated as **offline**.

- **`cluster.failover.status`** (read-only) reports each node's derived `health` (`online`/`offline`, distinct from its administrative status), the last-heartbeat age, whether the primary is healthy, and — when the primary is unreachable — the `recommended_candidate`: the freshest online replica (ties broken by `node_id` so every observer agrees). `cluster.status` also carries `primary_healthy` and `recommended_candidate`.
- The recommendation is **advisory**. Acting on it (promoting the candidate) is still done explicitly via `cluster.replica.promote`. Automated promotion is deliberately not performed yet: without a consensus/fencing layer, auto-promoting on a network partition could create two primaries (split-brain). Fenced automated failover is a planned follow-on.
