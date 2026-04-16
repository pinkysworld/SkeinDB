# PostgreSQL Compatibility

Last updated: 2026-03-31

Status: Partial baseline

SkeinDB now ships a PostgreSQL v3 wire protocol listener alongside the MySQL listener and HTTP control plane.
The current implementation is intentionally narrow: it is good for protocol bring-up, smoke tests, and exercising the shared SQL engine over a PG socket, but it is not yet full PostgreSQL compatibility.

## Quick start

```bash
# Trust auth when SKEINDB_TOKEN is unset
cargo run -- serve --data ./data --http 8080 --mysql 3306 --pg 5432

psql "host=127.0.0.1 port=5432 user=skein dbname=app sslmode=disable" -c "SELECT 1"

# If SKEINDB_TOKEN is set, use the same value as the password:
PGPASSWORD="$SKEINDB_TOKEN" \
  psql "host=127.0.0.1 port=5432 user=skein dbname=app sslmode=disable" \
  -c "SELECT version()"
```

Notes:
- `sslmode=disable` is recommended for now because the listener explicitly rejects PostgreSQL SSL negotiation with `N`.
- The PG listener shares the same underlying execution engine as MySQL and SkeinQL.

## Implemented today

- PostgreSQL v3 message framing in `pg_wire.rs`
- `StartupMessage` and `SSLRequest` parsing
- startup response batch: `AuthenticationOk` / `ParameterStatus` / `BackendKeyData` / `ReadyForQuery`
- trust auth when `SKEINDB_TOKEN` is unset
- cleartext-password auth path when `SKEINDB_TOKEN` is set
- SSL negotiation rejection (`'N'`)
- simple query protocol delegated to the shared SQL execution engine
- special-case startup/bootstrap query responses for `SELECT version()`, `current_database()`, `current_schema()`, `SHOW server_version` / `server_version_num` / `standard_conforming_strings` / `max_identifier_length`, `SHOW transaction isolation level`, and `SELECT current_setting(...)`
- empty-query handling
- `BEGIN` / `COMMIT` / `ROLLBACK` compatibility stubs
- `Terminate` handling
- extended-query protocol acknowledgements as stubs only

## Module map

| File | Status | Purpose |
|------|--------|---------|
| `pg_wire.rs` | Implemented | PG v3 message framing, startup parsing, backend message encode/write helpers, common PG type OIDs, unit tests |
| `server.rs` (PG section) | Implemented | PG listener, startup/auth flow, SSL rejection, simple query loop, transaction stubs, extended-protocol stubs |
| `pg_auth.rs` | Planned | SCRAM-SHA-256 and richer auth paths |
| `pg_session.rs` | Planned | `search_path`, `DateStyle`, `TimeZone`, tx-state tracking, `client_encoding` |
| `pg_parse.rs` | Planned | PG-specific SQL dialect features (`RETURNING`, `::`, `ILIKE`, arrays, dollar-quoting, `ON CONFLICT`) |
| `pg_types.rs` | Planned | Richer OID mapping plus text/binary format parity |
| `pg_catalog.rs` | Planned | Virtual `pg_catalog.*` tables |
| `pg_functions.rs` | Planned | PG-specific scalar/aggregate functions |

## Authentication

| Method | Status | Notes |
|--------|--------|-------|
| trust | Supported | Default when `SKEINDB_TOKEN` is not set |
| cleartext password | Supported | Uses `SKEINDB_TOKEN` as the password gate |
| SCRAM-SHA-256 | Planned | Backlog item T401 |
| md5 | Not planned | Prefer SCRAM once implemented |
| TLS client certs | Not implemented | SSL negotiation is currently rejected |

## Protocol surface

### Simple query protocol

The listener accepts `Query` messages and routes supported SQL into the shared execution engine.
For the common shared SQL subset, responses are encoded as:

- `RowDescription`
- zero or more `DataRow`
- `CommandComplete`
- `ReadyForQuery`

`SELECT version()` plus the common startup/bootstrap probes above are handled explicitly so PG clients can complete early compatibility checks before falling through to the shared engine.

### Extended query protocol

The listener currently accepts the extended-query message family only as a compatibility scaffold.
`Parse`, `Bind`, `Describe`, `Execute`, `Close`, and `Sync` are acknowledged with stub behavior, but real prepared-statement / portal semantics, parameter placeholders, and driver-grade lifecycle handling are still open work.

### Transactions

`BEGIN`, `COMMIT`, and `ROLLBACK` are accepted as compatibility stubs.
Full PostgreSQL transaction-state semantics, including richer `ReadyForQuery` state management (`I` / `T` / `E`) and failed-transaction blocks, are still open.

## Tested flows

Current integration coverage in `crates/skeindb/tests/cluster_rpc.rs` includes:

- startup handshake reaches `ReadyForQuery`
- simple query `SELECT 1`
- simple query `SELECT version()`
- startup/bootstrap query bundle covering `current_database()`, `current_schema()`, `SHOW server_version` / `server_version_num` / `standard_conforming_strings` / `max_identifier_length`, `SHOW transaction isolation level`, and `SELECT current_setting(...)`
- empty query returns the expected empty response flow
- `Terminate` closes the connection cleanly
- SSL negotiation is rejected correctly

`pg_wire.rs` also carries 20 unit tests for message framing and encode/decode behavior.

## Claimed version and startup behavior

- `SELECT version()` returns a PostgreSQL-flavored SkeinDB string
- common startup/bootstrap probes return PostgreSQL-shaped values for `current_database()`, `current_schema()`, `SHOW ...`, and `current_setting(...)`
- the listener emits startup `ParameterStatus` messages during connection setup
- the default port is `5432`
- `--pg 0` disables the listener

## Not implemented yet

- SCRAM-SHA-256 authentication
- PostgreSQL session state (`search_path`, `DateStyle`, `TimeZone`, `client_encoding`, `standard_conforming_strings`)
- PG-specific SQL dialect features such as `RETURNING`, `::` casts, dollar-quoting, `ILIKE`, arrays, `FETCH FIRST`, and `ON CONFLICT`
- `pg_catalog` system tables and broader PG bootstrap-query compatibility for tools/frameworks
- COPY protocol
- PostgreSQL SQLSTATE parity
- richer type encoding and binary format support
- production-grade driver compatibility for Django, Rails, SQLAlchemy, `pgAdmin`, `DBeaver`, `psycopg`, and `node-postgres`

## Architecture

Both SQL frontends target the same shared execution layer:

```text
MySQL (3306) ──┐
PG    (5432) ──┤──→ SqlPlan / SkeinQL IR ──→ Engine
HTTP  (8080) ──┘
```

## Backlog map

Phase 25 in `docs/PROJECT_BACKLOG.md` tracks the remaining PostgreSQL work:

- T400 / T403 / T410 / T418 are complete
- T401-T409 and T411-T417 remain open for auth hardening, PG parser work, catalogs, extended protocol, result typing, and driver compatibility

Use `docs/TRUE_STATUS_MATRIX.md` when you want the runtime-backed truth snapshot rather than the aspirational roadmap.
