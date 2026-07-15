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
| `SKEINDB_CLUSTER_AUTO_FAILOVER` | `0` (disabled) | Opt-in **automated fenced failover**. When enabled, a primary that has lost quorum refuses writes (fenced) and the majority-side elected candidate promotes itself and announces the new leadership. When disabled (default), failover is manual via `cluster.replica.promote` (which is still quorum-gated). Requires a **3+ node** cluster to keep write availability during a single failure (a 2-node cluster has no majority when one node is down). |

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
| `readwrite` | write | Reads **and** data mutations + table-level DDL (not database create/drop) |
| `readonly` | read | Read-only methods only |

Unknown or empty role strings fail safe to `read` (least privilege). Tokens created without an explicit role default to `admin`, so tokens issued before enabling RBAC keep full access.

**Method classification.** Read-only methods (e.g. `query.select`, `data.get`, `schema.list_tables`, `stats.*`) require `read`. Control-plane methods (`admin.user.*`, `security.token.*`, `cluster.*` mutations, `settings.set`, `settings.encryption.*`, `system.shutdown`, `maintenance.*.set_policy`, `maintenance.history.gc`, **`schema.create_database` / `schema.drop_database`**, …) require `admin` — including the *listing* ones (`admin.user.list`, `security.token.list`), since they expose security configuration. **Database provisioning is admin** because it changes the *set* of databases; managing schema and data *within* a database (table-level DDL, `data.*`) requires `write`. `sql.exec` is classified by whether the statement is read-only. Unknown methods fail safe to `write` (a `readonly` principal is denied). A denied call returns `403 forbidden` and is logged at WARN.

**Database scope.** Beyond the role, an API token can be restricted to specific databases. Pass `db_scope` to `security.token.create`:

```json
{"skeinql":"1.0","id":1,"method":"security.token.create",
 "params":{"role":"readwrite","label":"analytics-app","db_scope":["analytics"]}}
```

A **database-scoped** token may only perform data-plane operations on the databases in its scope. For each request the target database(s) are extracted and every one must be in scope, otherwise the call is denied with `403 forbidden`:

- **Reads/writes on a single database** — `data.get/insert/update/delete`, `schema.list_tables/describe_table/create_table/drop_table`, `vector.insert/search/index.status`, `view.create/drop/refresh`, `merge.apply`, and write-shaped `sql.exec` — checked against the referenced database. (Creating/dropping a whole database requires `admin`, so it is not available to a scoped token at all.)
- **`query.select`** — every database its `FROM` list touches (across joins, subqueries, set-operations, and CTE bodies) must be in scope; a cross-database query to an out-of-scope database is denied, naming that database.
- **Scope-neutral methods** — `system.ping/version/capabilities`, `transport.capabilities`, and `tx.begin/commit/rollback` — are always allowed for a scoped token.
- **Everything else** — global aggregates (`stats.*`), control-plane, and the prepared/patch/subscribe query variants — is **denied** for a scoped token (fail closed). Use an unscoped credential for those.

A token with no `db_scope` (the default, and every token created before this field existed) is unrestricted across databases. The role check always applies first: a `readonly` scoped token still cannot write, even within its own database.

**User credentials & per-database grants.** Besides API tokens, a **database user** (`admin.user.create`) can log in and be authorized by its per-database grants. Creating a user returns a one-time login `secret` (prefix `usr_`), presented as `Authorization: Bearer <secret>` just like a token:

```json
{"skeinql":"1.0","id":1,"method":"admin.user.create","params":{"username":"alice","role":"readwrite"}}
// → { "username": "alice", "role": "readwrite", "secret": "usr_…", "grants": {} }   (secret shown once)
{"skeinql":"1.0","id":2,"method":"admin.user.grant","params":{"username":"alice","db":"analytics","privileges":["write"]}}
```

The user's **role is the ceiling** and its **grants are the per-database allowlist**:

- An **`admin`-role** user is unrestricted across databases (like the superuser token).
- A **`readwrite`/`readonly`** user may only touch databases it has a grant for, and only up to the effective `min(role, grant)` privilege. Grant privilege strings map to `read` (`read`/`select`), `write` (`write`/`insert`/`update`/`delete`/`dml`), or `admin` (`all`/`ddl`/`grant`/`owner`); a target's effective privilege is the highest grant on it. A method targeting a database/table the user has no grant for is denied.
- Non-database methods are governed by the role alone; `admin.user.list` never returns secrets (only a `has_secret` flag).

**Per-table grants.** Grants are database-wide by default, but adding a `table` narrows a grant to one table (`db.table`), and a table-specific grant **takes precedence** over the whole-database grant for that table:

```json
// read the whole `analytics` database, but write only its `events` table
{"skeinql":"1.0","id":3,"method":"admin.user.grant","params":{"username":"alice","db":"analytics","privileges":["read"]}}
{"skeinql":"1.0","id":4,"method":"admin.user.grant","params":{"username":"alice","db":"analytics","table":"events","privileges":["write"]}}
```

For each `(database, table)` a method touches, authorization looks up the `db.table` grant first and falls back to the `db` grant. A whole-database operation (e.g. `schema.list_tables`) requires a database-level grant — a table-only grant does not confer it. (`admin.user.revoke` takes the same optional `table`. Token `db_scope` remains database-level.)

**Scope (current slice).** Role + per-token database scope + per-user database **and table** grants + an admin gate on database provisioning. Users created before the login secret existed have an empty secret and must be re-created to log in.

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

Each node's liveness is tracked by the age of its last heartbeat. When clustering is enabled, every node runs a background **heartbeat sender** that calls `cluster.node.heartbeat` (`{"node_id": "<self>", "applied_ops": <replication progress>}`) on all peers roughly every `SKEINDB_CLUSTER_NODE_TIMEOUT_MS / 3` (so a node misses several heartbeats before being aged out). A node whose last heartbeat is older than `SKEINDB_CLUSTER_NODE_TIMEOUT_MS` (default 15s) is treated as **offline**.

- **`cluster.failover.status`** (read-only) reports each node's derived `health` (`online`/`offline`, distinct from its administrative status), the last-heartbeat age, its last-reported `applied_ops` (replication progress — the signal the data-safe election uses, so a failover decision is auditable), whether the primary is healthy, the current `leadership_epoch`, and — when the primary is unreachable — the `recommended_candidate`: the most up-to-date online replica (highest applied replication progress, ties by heartbeat then `node_id` so every observer agrees). `cluster.status` also carries `primary_healthy` and `recommended_candidate`.
- **`cluster.shard.failover.status`** (read-only) reports the same readiness **per shard**: for every shard, its primary's health, its per-shard `leadership_epoch`, whether it holds a quorum of *its own* node set (primary + replicas), and — when its primary is down — the recommended replica to promote for that shard. Each shard is its own replication group and fails over independently.

### Quorum fencing (split-brain prevention)

`cluster.failover.status` also reports a `quorum` block: the majority `size` (`floor(members/2)+1`), how many members are currently `reachable`, whether this node `has_quorum`, and `primary_should_step_down` (true when the local primary has lost quorum).

- **Quorum-gated promotion.** A whole-cluster `cluster.replica.promote` is **refused** (`no_quorum`) unless the promoting node observes a quorum of the cluster. On a network partition only the majority side can reach a quorum, so two disjoint partitions can never both elect a primary. During a genuine multi-node outage where you must recover from the minority, pass `force: true` to override (accepting the risk).
- **Leadership epoch.** Every whole-cluster promotion increments a monotonic `leadership_epoch` — the fencing token that lets a superseded primary detect it is stale.
### Automated fenced failover (opt-in)

Set `SKEINDB_CLUSTER_AUTO_FAILOVER=1` to let the cluster fail over on its own. A background tick on every node evaluates its view and acts:

- **Write fencing.** A primary that has lost quorum **refuses writes** (`fenced` error) — it cannot diverge from the primary the majority side will elect. (With auto-failover off, writes are never fenced.)
- **Election with a vote round.** A replica that observes the primary as offline, holds a quorum, and is the deterministically elected candidate starts an election for the next **term**: it requests votes from all peers (`cluster.request_vote`) and **promotes itself only if a majority grant it**. Each node grants **at most one vote per term** (and never for a term that does not exceed the leader it already recognizes), which — by majority intersection — guarantees **at most one candidate can win a given term**. The winner promotes at that term (setting `leadership_epoch`) and announces the new leadership via `cluster.leader.announce`. A losing/split election is retried on a later tick with a higher term.
- **Data-safe candidate selection (log matching).** The election prefers the **most up-to-date** replica — the one with the highest applied replication progress (`applied_ops`, propagated on every heartbeat) — not merely the freshest heartbeat (ties fall back to heartbeat, then `node_id`). And a voter **refuses any candidate less caught up than itself** (the Raft up-to-date rule). Since a committed write is replicated to a majority and a winner needs a majority of votes, the elected primary is guaranteed to hold every committed write — so automatic failover cannot lose acknowledged data.
- **Epoch-guarded adoption.** Peers adopt an announced leader only if its epoch is **strictly newer** than their own (monotonic, idempotent — a stale or duplicate announce is ignored). A superseded primary demotes itself when it hears a newer leader (e.g. once a partition heals).

Because a candidate needs both a quorum and a majority of votes for its term — and two disjoint partitions can never both hold a majority — **at most one primary is ever elected per term and at most one accepts writes**. Run **3+ nodes** so the cluster keeps a majority (and write availability) when a single node fails.

**Per shard.** When databases/tables are sharded, the same machinery runs **independently per shard**: each shard has its own primary, leadership epoch, quorum (a majority of *that shard's* node set), and vote round (`cluster.request_vote` / `cluster.leader.announce` carry a `shard_id`). The failover tick evaluates every shard as well as the whole cluster, so a shard whose primary is down is failed over on its own — write-fenced when it loses its shard quorum, then re-elected among its replicas. `cluster.shard.failover.status` reports each shard's readiness.

The vote is persisted before it is granted, so a node cannot vote twice in one term even across a crash. This is a quorum-and-term-based election in the spirit of Raft leader election, with the log-matching / up-to-date rule described above; a full commit-index consensus (majority-ack + divergent-log reconciliation across a leadership change) is the remaining roadmap item (see docs/CLUSTERING.md §2.5).

### Self-healing replication

Replication ships each write from the primary to its replicas and now **heals replicas that fall behind** — no manual rebuild after a transient network blip or a late join.

- Every replicated write carries a primary-assigned log position `(term, seq)` (header `x-skeindb-replication-seq`), and the primary keeps a bounded in-memory **op-log ring buffer** of recent ops (capacity 4096).
- A replica tracks its **contiguous applied position** and, per incoming op, applies it in order, treats an already-seen op as an **idempotent no-op** (safe re-delivery), or detects a **gap** and leaves its position unchanged.
- A replica's background loop pulls whatever it is missing from the primary via **`cluster.replication.fetch`** (`{after_term, after_seq}` → the next ops in order) and applies them, so it converges to the primary automatically. Each node's position is visible in `cluster.replication.status` (`op_seq`, `last_applied_term`, `last_applied_seq`) and per node in `cluster.failover.status`.
- If a replica has fallen further behind than the buffer still retains, `cluster.replication.fetch` returns `resync_required: true` — that replica must be re-synced from a snapshot (backup/restore) rather than from the op-log.

Steady-state lag heals on its own; reconciling logs across a **leadership change** remains best-effort until the commit-index slice (§2.5).
