# PostgreSQL Compatibility

Last updated: 2026-04-17

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
- transaction-state tracking via `ReadyForQuery` (`I` / `T` / `E`)
- failed-transaction-block handling (`25P02`) with `COMMIT`-as-rollback behavior after aborted transactions
- `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT` wired to the current undo log
- PostgreSQL SQLSTATE mapping for common shared-engine failures such as undefined tables, undefined columns, unique violations, syntax errors, and unsupported features
- `Terminate` handling
- text-format extended query protocol for `Parse` / `Bind` / `Describe` / `Execute` / `Sync` / `Close` / `Flush`, including named prepared statements, named portals, `$1`/`$2` placeholders, statement/portal `Describe`, and sync-based recovery after extended-protocol execution errors
- PG compatibility corpus at `tests/compat/pg_corpus.sql` executed end-to-end over the live PG listener
- PG-specific operators: `||` (string concatenation), `~` / `~*` (regex match), `->` / `->>` (JSON access)
- PG-specific scalar functions: `gen_random_uuid()`, `date_trunc()`, `to_char()`, `pg_typeof()`, `string_to_array()`, `array_length()`, `array_upper()`, `array_lower()`, `clock_timestamp()`, `statement_timestamp()`, `transaction_timestamp()`
- PG-specific aggregate functions: `string_agg()`, `array_agg()`
- PG SQL dialect rewriting: `::` type casts, `$$dollar quoting$$`, `"double-quoted"` identifiers, `IS [NOT] DISTINCT FROM`, `FETCH FIRST n ROWS ONLY`, `ARRAY[…]` constructor
- PG DDL compatibility: `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` → auto-increment integer columns, `CREATE SCHEMA` → `CREATE DATABASE`, `CREATE INDEX CONCURRENTLY` (accepted/ignored), `CREATE INDEX IF NOT EXISTS`, `COMMENT ON` (silently accepted)

## Module map

| File | Status | Purpose |
|------|--------|---------|
| `pg_wire.rs` | Implemented | PG v3 message framing, startup parsing, backend message encode/write helpers, common PG type OIDs, unit tests |
| `server.rs` (PG section) | Implemented | PG listener, startup/auth flow, SSL rejection, simple query loop, transaction/savepoint state, SQLSTATE mapping, and text-format extended-query lifecycle handling |
| `pg_auth.rs` | Planned | SCRAM-SHA-256 and richer auth paths |
| `pg_session.rs` | Planned | `search_path`, `DateStyle`, `TimeZone`, tx-state tracking, `client_encoding` |
| `pg_parse.rs` | Implemented | PG SQL dialect rewriting layer (`pg_rewrite_sql`): `::` type casts → `CAST(… AS …)`, `$$dollar quoting$$` → single-quoted, `"double-quoted"` identifiers → backtick-quoted, `IS [NOT] DISTINCT FROM` → `null_safe_eq`, `FETCH FIRST n ROWS ONLY` → `LIMIT n`, `ARRAY[…]` → PG array literal string. `ILIKE` and boolean literals handled natively. `RETURNING` and `ON CONFLICT` deferred to DML extensions. |
| `pg_types.rs` | Planned | Richer OID mapping plus text/binary format parity |
| `pg_catalog.rs` | Partial | Virtual `pg_catalog.*` tables currently served through `server.rs` (`pg_database`, `pg_namespace`, `pg_type`, `pg_proc` stub, `pg_settings`, `pg_stat_activity`); dedicated module is still planned |
| `pg_functions.rs` | Partial | PG-specific scalar/aggregate functions now inline in `engine.rs` and `server.rs`: `||` concat, `~`/`~*` regex, `->` / `->>` JSON access, `gen_random_uuid`, `date_trunc`, `to_char`, `pg_typeof`, `string_to_array`, `array_length`, `array_upper`, `array_lower`, `clock_timestamp`, `statement_timestamp`, `transaction_timestamp`, `string_agg`, `array_agg` |

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

For the current shared-engine subset, `RowDescription` now advertises common inferred/result-planned types as PostgreSQL `BOOL`, `INT8`, `FLOAT8`, `TEXT`, `DATE`, `TIME`, `TIMESTAMP`, `JSONB`, `BYTEA`, or `UUID` when the shared engine exposes those schema or literal types instead of defaulting every column to text metadata.

`bytes` cells continue to flow through the shared engine as base64 in the internal JSON result surface, but the PG listener now normalizes them to PostgreSQL `bytea` text format (`\\x...`) on the wire.

`SELECT version()` plus the common startup/bootstrap probes above are handled explicitly so PG clients can complete early compatibility checks before falling through to the shared engine.

### Extended query protocol

The listener now supports the core text-format extended-query lifecycle:

- `Parse` stores named prepared statements and tracks declared parameter OIDs
- `Bind` stores named portals with text-format bound parameters
- `Describe` returns `ParameterDescription` plus statement/portal row metadata
- `Execute` substitutes `$1`/`$2` placeholders and routes through the shared SQL execution engine
- `Close`, `Sync`, and `Flush` behave as PG lifecycle messages rather than compatibility stubs

Current limits:

- parameter and result formats are text-only
- richer PG type OID coverage beyond the current `BOOL` / `INT8` / `FLOAT8` / `TEXT` / `DATE` / `TIME` / `TIMESTAMP` / `JSONB` / `BYTEA` / `UUID` baseline and binary format parity remain part of the open type/result-encoding work
- partial portal suspension (`Execute` with incremental row draining) is not implemented yet

### Transactions

The listener now tracks PostgreSQL-style `ReadyForQuery` states:

- `I` when idle
- `T` inside a transaction block
- `E` after an error aborts the current transaction block

While in the failed state, regular commands are rejected with `25P02` until the client issues `ROLLBACK`, `COMMIT` (which resolves as a rollback), or `ROLLBACK TO SAVEPOINT`.
`SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT` are backed by the current undo-log bookkeeping that already exists in the shared SQL layer.

## Tested flows

Current integration coverage in `crates/skeindb/tests/cluster_rpc.rs` includes:

- startup handshake reaches `ReadyForQuery`
- simple query `SELECT 1`
- simple query `SELECT version()`
- startup/bootstrap query bundle covering `current_database()`, `current_schema()`, `SHOW server_version` / `server_version_num` / `standard_conforming_strings` / `max_identifier_length`, `SHOW transaction isolation level`, and `SELECT current_setting(...)`
- simple-query `pg_catalog` round-trips for `pg_database`, `pg_namespace`, `pg_settings`, `pg_type`, `pg_stat_activity`, `pg_class`, `pg_attribute`, `pg_index`, and `pg_constraint`
- simple-query `RowDescription` OID checks for numeric, boolean, temporal, JSON, UUID, and text result columns
- extended query `Parse` / `Bind` / statement+portal `Describe` / `Execute` / `Close` / `Sync` / `Flush` round-trips
- extended-query `RowDescription` OID checks for described/executed result columns
- sync-based recovery after extended-query execution errors
- PG corpus execution from `tests/compat/pg_corpus.sql`
- failed transaction blocks move `ReadyForQuery` into `E`, reject follow-up commands with `25P02`, and roll back correctly on `COMMIT` / `ROLLBACK`
- `ROLLBACK TO SAVEPOINT` clears failed transaction state and restores the undo-log-backed transaction snapshot
- duplicate-key, undefined-table, and syntax-path execution errors surface PostgreSQL SQLSTATE codes
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
- PG-specific DML extensions such as `RETURNING`, `ON CONFLICT DO NOTHING/UPDATE`, and `COPY FROM STDIN / TO STDOUT`
- broader PG bootstrap-query compatibility for tools/frameworks (`pg_catalog.pg_class`, `pg_attribute`, `pg_index`, and `pg_constraint` are now implemented with table/index/column metadata derived from the shared catalog)
- COPY protocol
- richer type encoding and binary format support beyond the current `BOOL` / `INT8` / `FLOAT8` / `TEXT` / `DATE` / `TIME` / `TIMESTAMP` / `JSONB` / `BYTEA` / `UUID` metadata baseline
- partial portal suspension for incremental `Execute` row draining
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

- T400 / T403 / T408 / T410 / T411 / T412 / T413 / T414 / T415 / T418 are complete
- T401-T407 / T409 / T416-T417 remain open for auth hardening, PG parser work, richer type mapping, broader catalog coverage, and driver compatibility

Use `docs/TRUE_STATUS_MATRIX.md` when you want the runtime-backed truth snapshot rather than the aspirational roadmap.
