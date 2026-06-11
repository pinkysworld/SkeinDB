# Real-driver smoke matrix

These scripts boot a real `skeindb serve` process and connect to its MySQL and
PostgreSQL wire listeners using **genuine third-party client drivers** — not the
in-repo wire codecs. They are the end-to-end counterpart to the protocol unit
and integration tests: they prove that off-the-shelf drivers can authenticate
and run a full DDL/DML/SELECT round-trip against SkeinDB.

The orchestrator [`run_smoke.sh`](run_smoke.sh) starts the server on ephemeral
ports with a fresh data directory and an auth token, waits for `/health`, then
runs each driver script. A driver whose tooling is not installed is skipped,
unless `SKEINDB_SMOKE_STRICT=1` (set in CI), where a missing driver fails.

## Matrix

| Script | Driver | Protocol exercised | Auth |
| --- | --- | --- | --- |
| [`pg_psql.sh`](pg_psql.sh) | `psql` CLI | PostgreSQL simple query | SCRAM-SHA-256 |
| [`pg_psycopg.py`](pg_psycopg.py) | psycopg 3 | PostgreSQL extended query (parameterized) | SCRAM-SHA-256 |
| [`pg_node.mjs`](pg_node.mjs) | node-postgres (`pg`) | PostgreSQL simple + extended query | SCRAM-SHA-256 |
| [`mysql_cli.sh`](mysql_cli.sh) | `mysql` CLI | MySQL text protocol | caching_sha2_password |
| [`mysql_node.mjs`](mysql_node.mjs) | `mysql2` | MySQL text + binary prepared statement | caching_sha2_password |
| [`mysql_pymysql.py`](mysql_pymysql.py) | PyMySQL | MySQL text protocol | caching_sha2_password |

Each script uses a distinct database name (`smoke_psql`, `smoke_psycopg`,
`smoke_pgnode`, `smoke_mysqlcli`, `smoke_mysqlnode`, `smoke_pymysql`) so the
drivers do not collide and can run against one shared server.

Authentication runs over plaintext (clients use `sslmode=disable` /
`--ssl-mode=DISABLED`); SCRAM-SHA-256 and the caching_sha2_password fast-auth
path both complete without TLS.

## Running locally

```bash
# 1. Build the server binary.
cargo build -p skeindb --bin skeindb

# 2. Install the driver dependencies you have language runtimes for.
( cd tests/smoke && npm install )                 # node-postgres + mysql2
python -m pip install -r tests/smoke/requirements.txt   # psycopg3 + PyMySQL
# psql / mysql CLIs come from your OS package manager (optional locally).

# 3. Run the matrix.
bash tests/smoke/run_smoke.sh
```

Drivers without installed tooling are reported as `SKIP`. The run fails only if
an installed driver fails its round-trip.

## Environment knobs

| Variable | Default | Purpose |
| --- | --- | --- |
| `SKEINDB_BIN` | `target/{debug,release}/skeindb` | Path to the server binary |
| `SKEINDB_SMOKE_STRICT` | `0` | `1` makes missing tooling a failure (CI uses this) |
| `SKEINDB_SMOKE_TOKEN` | `smoke-matrix-secret` | Auth token / password |
| `SKEINDB_SMOKE_USER` | `skein` | Login user name |

## CI

The `driver-smoke` job in [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
installs all six drivers (Node, Python, and the `psql` / `mysql` CLIs) and runs
the matrix with `SKEINDB_SMOKE_STRICT=1`, so every driver must be present and
pass.

## Notes

- **MySQL scalar typing.** The MySQL text protocol returns scalar column values
  as strings, so the MySQL scripts compare values loosely (e.g. `str(x) == "1"`).
  The PostgreSQL scripts assert proper integer types because the PostgreSQL
  protocol carries them faithfully.
- **psycopg3 regression guard.** psycopg3 uses the extended-query protocol for
  every statement and strictly enforces that a `DataRow` is preceded by a single
  `RowDescription`. This matrix surfaced (and now guards against) a server bug
  where `Execute` emitted a duplicate `RowDescription` after a portal `Describe`;
  the wire-level regression test lives in
  [`crates/skeindb/tests/cluster_rpc.rs`](../../crates/skeindb/tests/cluster_rpc.rs).
