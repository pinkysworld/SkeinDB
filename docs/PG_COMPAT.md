# PostgreSQL Compatibility

Last updated: 2026-06-01

Status: Partial advanced baseline

SkeinDB now ships a PostgreSQL v3 wire protocol listener alongside the MySQL listener and HTTP control plane.
The current implementation is intentionally narrow: it is good for protocol bring-up, smoke tests, and exercising the shared SQL engine over a PG socket, but it is not yet full PostgreSQL compatibility.

## Quick start

```bash
# Trust auth when SKEINDB_TOKEN is unset
cargo run -- serve --data ./data --http 8080 --mysql 3306 --pg 5432

psql "host=127.0.0.1 port=5432 user=skein dbname=app sslmode=disable" -c "SELECT 1"

# If SKEINDB_TOKEN is set, the listener uses SCRAM-SHA-256 and
# the password is the token value:
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
- SCRAM-SHA-256 auth path when `SKEINDB_TOKEN` is set
- SSL negotiation rejection (`'N'`)
- PG session settings for common `SET` / `SET ... TO` / `RESET` / `RESET ALL`, with `SHOW`, `current_setting(name)`, `current_setting(name, missing_ok)`, `current_schema` (with optional parentheses), and `current_schemas(bool)` reading the effective `search_path`
- simple query protocol delegated to the shared SQL execution engine
- special-case startup/bootstrap query responses for `SELECT version()`, `current_database()`, `current_catalog`, `current_schema` (with optional parentheses), `current_schemas(bool)`, `current_user`, `current_role`, `session_user`, `user`, `SHOW server_version` / `server_version_num` / `standard_conforming_strings` / `max_identifier_length`, `SHOW transaction isolation level`, and `SELECT current_setting(...)` including the `missing_ok` form
- empty-query handling
- transaction-state tracking via `ReadyForQuery` (`I` / `T` / `E`)
- failed-transaction-block handling (`25P02`) with `COMMIT`-as-rollback behavior after aborted transactions
- `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT` wired to the current undo log
- PostgreSQL SQLSTATE mapping for common shared-engine failures such as undefined tables, undefined columns, unique violations, syntax errors, and unsupported features
- `Terminate` handling
- extended query protocol for `Parse` / `Bind` / `Describe` / `Execute` / `Sync` / `Close` / `Flush`, including named prepared statements, named portals, `$1`/`$2` placeholders, statement/portal `Describe`, text **and binary** bind parameters for common scalar OIDs (`int2`/`int4`/`int8`, `float4`/`float8`, `bool`, `numeric`, `date`/`time`/`timestamp`/`timestamptz`, `uuid`, `bytea`, and `text`/`varchar`/`json`/`jsonb`), binary result-format encoding for common scalar OIDs, and sync-based recovery after extended-protocol execution errors
- PG compatibility corpus at `tests/compat/pg_corpus.sql` executed end-to-end over the live PG listener; every statement must complete without an `ErrorResponse`, and a curated subset of deterministic literal/scalar expressions is checked for exact values. Coverage spans startup probes, savepoints, `::`/`CAST` type casts, dollar-quoted literals (`$$…$$`, `$tag$…$tag$`), `ARRAY[…]` constructors, JSON/JSONB `->`/`->>`, column-side regex `~`/`~*`, `FETCH FIRST`, aggregates (`COUNT`/`SUM`/`AVG`/`string_agg`/`array_agg`), `GROUP BY`/`HAVING`, joins, `CASE`/`COALESCE`/`NULLIF`, subqueries/CTEs/`UNION`, scalar string/math functions, and DML with `RETURNING` and `ON CONFLICT … DO UPDATE`
- real-driver smoke matrix at `tests/smoke/` that boots `skeindb serve` and round-trips through `psql`, psycopg3, and node-postgres (alongside the MySQL drivers), gated as the `driver-smoke` CI job
- PG-specific operators: `||` (string concatenation), `~` / `~*` (regex match), `->` / `->>` (JSON access)
- PG-specific scalar functions: `gen_random_uuid()`, `date_trunc()`, `to_char()`, `pg_typeof()`, `string_to_array()`, `array_length()`, `array_upper()`, `array_lower()`, `clock_timestamp()`, `statement_timestamp()`, `transaction_timestamp()`
- driver-introspection helpers that return deterministic single-tenant answers: `format_type(oid, typmod)` for common base/array types (with `varchar(n)`, `numeric(p,s)`, and time/timestamp precision modifiers), the `pg_*_is_visible(oid)` visibility probes (`pg_table_is_visible`, `pg_type_is_visible`, `pg_function_is_visible`, and the text-search/operator/opclass/collation/conversion variants), the `has_*_privilege(...)` probes (`has_table_privilege`, `has_column_privilege`, `has_schema_privilege`, `has_database_privilege`, `has_sequence_privilege`, `has_function_privilege`, and friends) which always succeed for the owner role, `pg_get_userbyid(oid)` returning the bootstrap role, and `pg_encoding_to_char(int)` returning `UTF8`
- Common shared-engine string functions over the PG path: `lower()`, `upper()`, `length()`, `char_length()`, `trim()`, `ltrim()`, `rtrim()`, `left()`, `right()`, `substring()`, `replace()`
- PG-specific aggregate functions: `string_agg()`, `array_agg()`
- PG SQL dialect rewriting: `::` type casts, `$$dollar quoting$$`, `"double-quoted"` identifiers, `IS [NOT] DISTINCT FROM`, `FETCH FIRST n ROWS ONLY`, `ARRAY[…]` constructor, and `ON CONFLICT` rewrite support for the current shared-engine DML subset
- `INSERT` / `UPDATE` / `DELETE ... RETURNING` extraction with supported follow-up reads, plus default/text/csv `COPY table [ (col, ...) ] TO STDOUT`, `COPY table [ (col, ...) ] FROM STDIN`, `COPY (SELECT ...) TO STDOUT`, and binary `COPY ... TO STDOUT` / `COPY ... FROM STDIN` over both simple and extended query flows, with explicit `WITH (FORMAT text)`, `WITH (FORMAT csv)`, and `WITH (FORMAT binary)` plus PostgreSQL-style `WITH (TEXT)`, `WITH (CSV)`, and `WITH (BINARY)` aliases and legacy bare `WITH TEXT`, `WITH CSV`, and `WITH BINARY` forms, text/csv `NULL '...'`, CSV `HEADER`, CSV `HEADER MATCH` on `COPY ... FROM STDIN`, and single-byte `DELIMITER`, `QUOTE`, and `ESCAPE` on supported CSV forms; binary `COPY FROM STDIN` decodes the standard PostgreSQL binary stream (signature, flags, per-tuple field framing) for common scalar types (integers, floats, `bool`, `uuid`, text), while most other COPY options still return `0A000`
- PG DDL compatibility: `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` → auto-increment integer columns, `CREATE SCHEMA` → `CREATE DATABASE`, `CREATE INDEX CONCURRENTLY` (accepted/ignored), `CREATE INDEX IF NOT EXISTS`, `COMMENT ON` (silently accepted)
- virtual `pg_catalog` coverage for `pg_database`, `pg_namespace`, `pg_tables`, `pg_views`, `pg_roles`, `pg_authid`, `pg_user`, `pg_group`, `pg_tablespace`, `pg_am`, `pg_description`, `pg_indexes`, `pg_matviews`, `pg_sequences`, `pg_stats`, `pg_class`, `pg_attribute`, `pg_type`, `pg_index`, `pg_constraint`, `pg_proc` (basic builtin metadata for bootstrap functions plus selected PG scalar/aggregate functions including timestamp, UUID, array, formatting, type, aggregate, and common string helpers, with aggregate `prokind` rows, the current `substring` arities, the current `current_setting` arities, and `current_schemas(bool)`), `pg_settings`, `pg_stat_activity`, and `pg_stat_database`
- shared `information_schema.columns` introspection through `sql.exec`, including common metadata fields (`COLUMN_TYPE`, character length, numeric precision/scale, charset/collation, privileges, comments, generated expression, and `EXTRA`) used by cross-dialect ORMs

## Module map

| File | Status | Purpose |
|------|--------|---------|
| `pg_wire.rs` | Implemented | PG v3 message framing, startup parsing, backend message encode/write helpers, common PG type OIDs, unit tests |
| `server.rs` (PG section) | Implemented | PG listener, startup/auth flow, SSL rejection, simple + extended query loops, transaction/savepoint state, SQLSTATE mapping, PG DML/DDL rewrites, and virtual catalog dispatch |
| `pg_auth.rs` | Inline implementation | SCRAM-SHA-256 lives in `pg_wire::scram` plus `server.rs` connection handling rather than a separate module |
| `pg_session.rs` | Inline implementation | Common PG settings live on `MySqlSessionState`; `SET`, `RESET`, `SHOW`, the current one- and two-argument `current_setting(...)` forms, and the `current_schema` (with optional parentheses) / `current_schemas(bool)` helpers use that session map, while startup-role bootstrap helpers preserve `current_user` / `current_role` / `session_user` / `user` from the connection username and map `current_catalog` to the current database |
| `pg_parse.rs` | Inline implementation | PG SQL dialect rewriting layer (`pg_rewrite_sql` + helpers in `server.rs`): `::` type casts, dollar quoting, double-quoted identifiers, `IS [NOT] DISTINCT FROM`, `FETCH FIRST`, `ARRAY[...]`, `ON CONFLICT`, and supported `RETURNING` extraction |
| `pg_types.rs` | Inline implementation | Common PG type OIDs, array OIDs, text encoding, binary result encoding, and binary bind-parameter decoding live in `pg_wire.rs` plus server-side inference helpers |
| `pg_catalog.rs` | Inline implementation | Virtual `pg_catalog.*` tables are served through the shared executor for `pg_database`, `pg_namespace`, `pg_tables`, `pg_views`, `pg_roles`, `pg_authid`, `pg_user`, `pg_group`, `pg_tablespace`, `pg_am`, `pg_description`, `pg_indexes`, `pg_matviews`, `pg_sequences`, `pg_stats`, `pg_class`, `pg_attribute`, `pg_type`, `pg_index`, `pg_constraint`, `pg_proc` (basic metadata for `version`, `current_database`, `current_schema`, `current_schemas`, `current_setting` with both current arities, `date_trunc`, `pg_typeof`, `to_char`, `gen_random_uuid`, `clock_timestamp`, `statement_timestamp`, `transaction_timestamp`, `string_to_array`, `split_part`, `array_length`, `array_upper`, `array_lower`, `string_agg`, `array_agg`, `lower`, `upper`, `length`, `char_length`, `trim`, `ltrim`, `rtrim`, `left`, `right`, `substring` with both 2-arg and 3-arg rows, and `replace`, including aggregate `prokind` rows), `pg_settings`, `pg_stat_activity`, and `pg_stat_database` |
| `pg_functions.rs` | Partial | PG-specific scalar/aggregate functions now inline in `engine.rs` and `server.rs`: `||` concat, `~`/`~*` regex, `->` / `->>` JSON access, `gen_random_uuid`, `date_trunc`, `to_char`, `pg_typeof`, `string_to_array`, `split_part`, `array_length`, `array_upper`, `array_lower`, `clock_timestamp`, `statement_timestamp`, `transaction_timestamp`, `string_agg`, `array_agg` |

## Authentication

| Method | Status | Notes |
|--------|--------|-------|
| trust | Supported | Default when `SKEINDB_TOKEN` is not set |
| SCRAM-SHA-256 | Supported | Used when `SKEINDB_TOKEN` is set; the token value is the PostgreSQL password |
| cleartext password | Legacy helper only | Wire helper exists, but the live listener now prefers SCRAM for token-protected PG sessions |
| md5 | Not planned | Prefer SCRAM |
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

`COPY ... TO STDOUT` is now implemented in default/text/csv and binary formats for plain table exports and parenthesized `SELECT` queries. CSV exports additionally support `HEADER`, which emits a first `CopyData` frame with the selected column names before row data, and text/csv exports accept a single-byte `DELIMITER` override plus custom `NULL '...'` markers for row encoding. Supported CSV forms also accept single-byte `QUOTE` and `ESCAPE` overrides for export escaping and import parsing. Binary exports switch `CopyOutResponse` and column format codes to `1`, emit the PostgreSQL binary stream header, one binary-encoded row payload per `CopyData`, and the standard trailer before `CopyDone`. Binary `COPY ... FROM STDIN` is the mirror of this: `CopyInResponse` advertises overall and per-column binary format `1`, the buffered `CopyData` payload is validated against the `PGCOPY\n\xff\r\n\0` signature and flags (the OID bit is rejected), and each tuple's per-field length-prefixed values are decoded by column kind (big-endian integers and floats by width, `bool`, hyphenated `uuid`, and UTF-8 text) before being inserted through the normal write path.

Default/text/csv `COPY table [ (col, ...) ] TO STDOUT`, `COPY table [ (col, ...) ] FROM STDIN`, `COPY (SELECT ...) TO STDOUT`, and binary `COPY ... TO STDOUT` / `COPY ... FROM STDIN` are implemented over both simple-query and extended-query flows, with optional table-column lists on table targets and explicit `WITH (FORMAT text)`, `WITH (FORMAT csv)`, and `WITH (FORMAT binary)` plus PostgreSQL-style `WITH (TEXT)`, `WITH (CSV)`, and `WITH (BINARY)` aliases and legacy bare `WITH TEXT`, `WITH CSV`, and `WITH BINARY` forms accepted on supported shapes. `COPY FROM STDIN WITH (FORMAT csv, HEADER)` skips the first CSV row before row validation and insert assembly, `COPY FROM STDIN WITH (FORMAT csv, HEADER MATCH)` additionally rejects mismatched header names before inserts are built, and custom delimiters, quote markers, escape markers, plus `NULL` markers are threaded through both export and import parsing. The backend emits `CopyInResponse`, buffers incoming `CopyData` chunks, and commits them as one multi-row insert on `CopyDone`; extended-query completion still waits for `Sync` before `ReadyForQuery`.

### Extended query protocol

The listener now supports the core extended-query lifecycle:

- `Parse` stores named prepared statements and tracks declared parameter OIDs
- `Bind` stores named portals with text- and binary-format bound parameters and requested result formats. Per-parameter format codes are honored, so drivers that mix text and binary parameters in one `Bind` work. Binary parameters are decoded by their declared OID: big-endian `int2`/`int4`/`int8`, IEEE-754 `float4`/`float8`, single-byte `bool`, base-10000 `numeric` (including `NaN`/`±Infinity`), `date`/`time`/`timestamp`/`timestamptz` against the PostgreSQL 2000-01-01 epoch, 16-byte `uuid`, raw `bytea`, and UTF-8 `text`/`varchar`/`json`/`jsonb` (the `jsonb` one-byte version header is validated and stripped). Malformed binary payloads are rejected with `22P03`.
- `Describe` returns `ParameterDescription` plus statement/portal row metadata; a portal `Describe` reflects the result-format codes requested in `Bind`, so binary result columns are advertised correctly
- `Execute` substitutes `$1`/`$2` placeholders and routes through the shared SQL execution engine; binary result-format requests are encoded for common scalar OIDs. When the portal was already described, `Execute` does **not** repeat the `RowDescription` (matching PostgreSQL, where the row layout is reported once by `Describe`). This fixes strict drivers such as psycopg3, which abort if a `DataRow` arrives without exactly one preceding `RowDescription`.
- `Close`, `Sync`, and `Flush` behave as PG lifecycle messages rather than compatibility stubs

Current limits:

- binary bind parameters cover the common scalar OID baseline above; unknown OIDs sent in binary are decoded as UTF-8 text, and other binary types are not yet interpreted
- binary result encoding is limited to the current common scalar OID baseline
- `Execute` with `max_rows > 0` now suspends and resumes prepared `SELECT` result sets via `PortalSuspended`; broader incremental row draining remains limited

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
- simple-query `COPY (SELECT ...) TO STDOUT` round-trip with `CopyOutResponse` / `CopyData` / `CopyDone`
- simple-query `COPY table FROM STDIN` round-trip with `CopyInResponse` and follow-up row verification
- simple-query `COPY table (col, ...) TO STDOUT` round-trip with column projection
- simple-query `COPY table (col, ...) FROM STDIN` round-trip with sparse-column inserts
- simple-query `COPY ... TO STDOUT WITH (FORMAT text)` round-trip
- simple-query `COPY ... FROM STDIN WITH (FORMAT text)` round-trip
- simple-query `COPY ... TO STDOUT WITH (FORMAT csv)` round-trip
- simple-query `COPY ... FROM STDIN WITH (FORMAT csv)` round-trip
- simple-query `COPY ... TO STDOUT WITH (FORMAT csv, HEADER)` round-trip
- simple-query `COPY ... TO STDOUT WITH (CSV, HEADER)` round-trip using PostgreSQL keyword-style format aliases
- simple-query `COPY ... TO STDOUT WITH CSV HEADER` round-trip using legacy bare `WITH` syntax
- simple-query `COPY ... TO STDOUT WITH (FORMAT csv, DELIMITER ';')` round-trip
- simple-query `COPY ... TO/FROM STDOUT/STDIN WITH (FORMAT csv, QUOTE '|')` round-trip
- simple-query `COPY ... TO/FROM STDOUT/STDIN WITH (FORMAT csv, QUOTE '|', ESCAPE '!')` round-trip
- simple-query `COPY ... FROM STDIN WITH (CSV)` round-trip using PostgreSQL keyword-style format aliases
- simple-query `COPY ... TO/FROM STDOUT/STDIN WITH (FORMAT csv, NULL 'NULL')` round-trip covering literal `"NULL"`, unquoted nulls, and empty strings
- simple-query `COPY ... FROM STDIN WITH (FORMAT csv, HEADER MATCH)` mismatch rejection before inserts are assembled
- simple-query `COPY ... TO STDOUT WITH (FORMAT binary)` round-trip with binary `CopyOutResponse` and decoded row payloads
- simple-query `COPY ... FROM STDIN WITH (FORMAT binary)` round-trip with binary stream decoding and row verification
- extended-query `COPY table (col, ...) TO STDOUT WITH (FORMAT text)` round-trip with `Parse` / `Bind` / `Execute` / `CopyOutResponse`
- extended-query `COPY (SELECT ...) TO STDOUT WITH (FORMAT text)` round-trip with `Parse` / `Bind` / `Execute` / `CopyOutResponse`
- extended-query `COPY table FROM STDIN` round-trip with `Parse` / `Bind` / `Execute` / `CopyInResponse` / `CopyDone` / `Sync`
- extended-query `COPY table (col, ...) FROM STDIN WITH (FORMAT text)` round-trip with `Parse` / `Bind` / `Execute` / `CopyInResponse` / `CopyDone` / `Sync`
- extended-query `COPY ... TO STDOUT WITH (FORMAT csv)` round-trip with `Parse` / `Bind` / `Execute` / `CopyOutResponse`
- extended-query `COPY ... TO STDOUT WITH (FORMAT binary)` round-trip with `Parse` / `Bind` / `Execute` / binary `CopyOutResponse`
- extended-query `COPY ... FROM STDIN WITH (FORMAT binary)` round-trip with `Parse` / `Bind` / `Execute` / binary `CopyInResponse` / `CopyDone` / `Sync`
- extended-query `COPY ... FROM STDIN WITH (FORMAT csv)` round-trip with `Parse` / `Bind` / `Execute` / `CopyInResponse` / `CopyDone` / `Sync`
- extended-query `COPY ... FROM STDIN WITH (FORMAT csv, HEADER)` round-trip with `Parse` / `Bind` / `Execute` / `CopyInResponse` / `CopyDone` / `Sync`
- extended-query `COPY ... FROM STDIN WITH (FORMAT csv, HEADER MATCH)` mismatch rejection with `Parse` / `Bind` / `Execute` / `CopyInResponse` / `Sync`
- extended-query `COPY ... FROM STDIN WITH (FORMAT csv, DELIMITER ';')` round-trip with `Parse` / `Bind` / `Execute` / `CopyInResponse` / `CopyDone` / `Sync`
- extended-query `COPY ... TO/FROM STDOUT/STDIN WITH (FORMAT csv, NULL 'NULL')` round-trip covering literal `"NULL"`, unquoted nulls, and empty strings
- startup/bootstrap query bundle covering `current_database()`, `current_catalog`, `current_schema` (with optional parentheses), `current_schemas(bool)`, `current_user`, `current_role`, `session_user`, `user`, `SHOW server_version` / `server_version_num` / `standard_conforming_strings` / `max_identifier_length`, `SHOW transaction isolation level`, and `SELECT current_setting(...)`
- simple-query `pg_catalog` round-trips for `pg_database`, `pg_namespace`, `pg_tables`, `pg_views`, `pg_roles`, `pg_authid`, `pg_user`, `pg_group`, `pg_tablespace`, `pg_am`, `pg_description`, `pg_indexes`, `pg_matviews`, `pg_sequences`, `pg_stats`, `pg_settings`, `pg_type`, `pg_stat_activity`, `pg_stat_database`, `pg_class`, `pg_attribute`, `pg_index`, and `pg_constraint`
- simple-query `RowDescription` OID checks for numeric, boolean, temporal, JSON, UUID, and text result columns
- extended query `Parse` / `Bind` / statement+portal `Describe` / `Execute` / `Close` / `Sync` / `Flush` round-trips
- extended-query portal suspension/resume for prepared `SELECT` statements when `Execute` uses `max_rows > 0`
- extended-query `RowDescription` OID checks for described/executed result columns
- sync-based recovery after extended-query execution errors
- PG corpus execution from `tests/compat/pg_corpus.sql`
- failed transaction blocks move `ReadyForQuery` into `E`, reject follow-up commands with `25P02`, and roll back correctly on `COMMIT` / `ROLLBACK`
- `ROLLBACK TO SAVEPOINT` clears failed transaction state and restores the undo-log-backed transaction snapshot
- duplicate-key, undefined-table, and syntax-path execution errors surface PostgreSQL SQLSTATE codes
- empty query returns the expected empty response flow
- `Terminate` closes the connection cleanly
- SSL negotiation is rejected correctly

`pg_wire.rs` also carries 24 unit tests for message framing and encode/decode behavior.

## Claimed version and startup behavior

- `SELECT version()` returns a PostgreSQL-flavored SkeinDB string
- common startup/bootstrap probes return PostgreSQL-shaped values for `current_database()`, `current_catalog`, `current_schema` (with optional parentheses), `current_schemas(bool)`, `current_user`, `current_role`, `session_user`, `user`, `SHOW ...`, and `current_setting(...)`
- the listener emits startup `ParameterStatus` messages during connection setup
- the default port is `5432`
- `--pg 0` disables the listener

## Not implemented yet

- broad PostgreSQL dialect parity beyond the current rewrite layer and corpus-backed subset
- most COPY options beyond the current `FORMAT text` / `FORMAT csv` / `FORMAT binary`, parenthesized and legacy bare `TEXT` / `CSV` / `BINARY` aliases, text/csv `NULL`, CSV `HEADER`, CSV `HEADER MATCH`, and single-byte `DELIMITER` / `QUOTE` / `ESCAPE` support
- broader type/array/domain encoding beyond the current scalar and array OID baseline
- broader portal suspension beyond prepared `SELECT` result sets
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

- T400-T418 are complete in the core roadmap checklist.
- Remaining PG work is no longer represented by unchecked Phase 25 boxes; track it as follow-up compatibility hardening: broader dialect coverage, additional COPY options/formats, broader portal suspension, larger catalog/driver matrices, and framework-specific smoke suites.

Use `docs/TRUE_STATUS_MATRIX.md` when you want the runtime-backed truth snapshot rather than the aspirational roadmap.
