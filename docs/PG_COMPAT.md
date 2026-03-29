# PostgreSQL Compatibility

SkeinDB offers a PostgreSQL v3 wire protocol listener alongside the existing MySQL listener.
Both protocols translate SQL into the shared SkeinQL IR and execute against the same Engine — no engine changes are required.

## Quick start

```bash
# skeindb-config.json already ships with pg_port: 5432
cargo run

# In another terminal
psql -h 127.0.0.1 -p 5432 -U skein -d app -c "SELECT 1"
```

## Architecture

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  MySQL 3306  │   │  PG   5432   │   │  HTTP  8080  │
│  (server.rs) │   │  (pg_*.rs)   │   │  (server.rs) │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                   │
       ▼                  ▼                   ▼
  ┌──────────────────────────────────────────────┐
  │             SqlPlan / SkeinQL IR             │
  └────────────────────┬─────────────────────────┘
                       ▼
              ┌──────────────────┐
              │      Engine      │
              │  (engine.rs)     │
              └──────────────────┘
```

### Module layout

| File | Purpose |
|------|---------|
| `pg_wire.rs` | PG v3 message framing: 1-byte tag + 4-byte BE length + payload |
| `pg_auth.rs` | SCRAM-SHA-256 (RFC 5802/7677) + trust authentication |
| `pg_session.rs` | `PgSessionState`: search_path, DateStyle, TimeZone, tx state (I/T/E) |
| `pg_connection.rs` | `handle_pg_connection()` + `run_pg_listener()` |
| `pg_parse.rs` | PG SQL dialect → SqlPlan translation |
| `pg_types.rs` | TypeDesc ↔ PG OID mapping + text/binary encoding |
| `pg_catalog.rs` | Virtual `pg_catalog.*` system tables |
| `pg_functions.rs` | PG-specific scalar/aggregate function implementations |

## Configuration

```json
{
  "pg_port": 5432
}
```

Set `pg_port` to `0` to disable the PostgreSQL listener.

## Claimed version

SkeinDB identifies itself as **PostgreSQL 16.0 (SkeinDB compatibility)** via `server_version` and `SELECT version()`.

## Authentication

| Method | Status | Notes |
|--------|--------|-------|
| trust | Supported | Default when `SKEINDB_TOKEN` is not set |
| SCRAM-SHA-256 | Supported | Password from `SKEINDB_TOKEN` env var |
| md5 | Not supported | Legacy; use SCRAM-SHA-256 instead |
| certificate | Not supported | — |

## Wire protocol

### Simple query protocol

`Query` message → parse SQL → execute → `RowDescription` + `DataRow`* + `CommandComplete` + `ReadyForQuery`.

### Extended query protocol

`Parse` → `Bind` → `Describe` → `Execute` → `Sync` cycle.
Supports named and unnamed statements/portals, `$1`/`$2` parameter placeholders.

### COPY protocol

Basic `COPY ... FROM STDIN` (CSV/text) and `COPY ... TO STDOUT` for bulk data transfer.

## SQL dialect

### Supported PG-specific syntax

| Feature | Example | Notes |
|---------|---------|-------|
| Double-quoted identifiers | `SELECT "Column" FROM "Table"` | MySQL uses backticks |
| Dollar-quoting | `$$text$$`, `$tag$text$tag$` | String literals |
| Type casts | `'5'::int`, `CAST('5' AS int)` | Both forms |
| RETURNING | `INSERT ... RETURNING *` | INSERT/UPDATE/DELETE |
| ILIKE | `WHERE name ILIKE '%foo%'` | Case-insensitive LIKE |
| IS DISTINCT FROM | `WHERE a IS DISTINCT FROM b` | NULL-safe inequality |
| Boolean literals | `TRUE`, `FALSE` | Not `1`/`0` |
| ARRAY literals | `ARRAY[1, 2, 3]` | Array constructor |
| ON CONFLICT | `INSERT ... ON CONFLICT DO UPDATE SET ...` | UPSERT |
| FETCH FIRST | `FETCH FIRST 10 ROWS ONLY` | Alternative to LIMIT |
| SERIAL / BIGSERIAL | `id SERIAL PRIMARY KEY` | Maps to auto_increment |

### Supported DDL

- `CREATE TABLE` / `DROP TABLE` / `ALTER TABLE` (add/drop/rename column, add/drop constraint)
- `CREATE INDEX` / `CREATE INDEX CONCURRENTLY` (concurrently accepted, ignored)
- `CREATE SCHEMA` → maps to database
- `CREATE VIEW` / `DROP VIEW`
- `COMMENT ON TABLE|COLUMN`

### Supported DML

- `INSERT` / `INSERT ... RETURNING` / `INSERT ... ON CONFLICT`
- `UPDATE` / `UPDATE ... RETURNING`
- `DELETE` / `DELETE ... RETURNING`
- `SELECT` with full expression support (joins, subqueries, CTEs, UNION, aggregates, window functions)

## System catalogs

| Catalog table | Status |
|---------------|--------|
| `pg_catalog.pg_database` | Supported |
| `pg_catalog.pg_namespace` | Supported |
| `pg_catalog.pg_class` | Supported |
| `pg_catalog.pg_attribute` | Supported |
| `pg_catalog.pg_type` | Supported |
| `pg_catalog.pg_index` | Supported |
| `pg_catalog.pg_constraint` | Supported |
| `pg_catalog.pg_proc` | Stub (empty) |
| `pg_catalog.pg_settings` | Supported |
| `pg_catalog.pg_stat_activity` | Supported |
| `information_schema.tables` | Supported |
| `information_schema.columns` | Supported |
| `information_schema.schemata` | Supported |

## PG-specific functions

### String
`string_agg()`, `split_part()`, `encode()`/`decode()`, `gen_random_uuid()`, `to_char()`, `to_number()`

### Date/Time
`now()`, `current_timestamp`, `extract(epoch FROM ...)`, `age()`, `date_trunc()`, `to_date()`, `to_timestamp()`

### JSON
`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `jsonb_build_object()`, `jsonb_agg()`, `jsonb_array_elements()`, `json_each()`, `jsonb_set()`, `jsonb_insert()`

### Array
`array_length()`, `unnest()`, `array_cat()`, `array_append()`, `array_agg()`, `ANY(array)`, `ALL(array)`

### Aggregate
`string_agg()`, `array_agg()`, `bool_and()`, `bool_or()`, `every()`

## Type mapping

| PG type | OID | SkeinQL TypeDesc | Notes |
|---------|-----|------------------|-------|
| `boolean` | 16 | `bool` | |
| `smallint` | 21 | `i16` | |
| `integer` | 23 | `i32` | |
| `bigint` | 20 | `i64` | |
| `real` | 700 | `f32` | |
| `double precision` | 701 | `f64` | |
| `text` | 25 | `string` | |
| `varchar(n)` | 1043 | `string` | Length enforced |
| `bytea` | 17 | `bytes` | |
| `json` | 114 | `json` | |
| `jsonb` | 3802 | `json` | Stored as JSON |
| `timestamp` | 1114 | `timestamp` | |
| `date` | 1082 | `date` | |
| `time` | 1083 | `time` | |
| `uuid` | 2950 | `string` | UUID format validated |
| `numeric` | 1700 | `f64` | Approximate |
| `serial` | 23 | `u32` + auto_increment | |
| `bigserial` | 20 | `u64` + auto_increment | |

## Transaction behavior

PostgreSQL uses implicit transactions: each statement auto-commits unless inside an explicit `BEGIN` block.

The `ReadyForQuery` message encodes transaction state:
- `I` — idle (no transaction)
- `T` — in transaction block
- `E` — failed transaction (all statements error until `ROLLBACK`)

## SQLSTATE error codes

| Code | Meaning |
|------|---------|
| `42P01` | Undefined table |
| `42703` | Undefined column |
| `23505` | Unique violation |
| `23502` | Not-null violation |
| `42601` | Syntax error |
| `42P06` | Duplicate schema |
| `42P07` | Duplicate table |
| `25P02` | In failed SQL transaction |
| `08003` | Connection does not exist |

## Known gaps

- **LISTEN/NOTIFY**: Not yet implemented. Plan: map to SkeinDB CDC subscriptions.
- **PL/pgSQL**: `DO $$ ... $$` blocks are accepted but return "not supported" error.
- **Large Objects (lo_\*)**: Not implemented.
- **Cursors (DECLARE/FETCH/CLOSE)**: Extended query portals are supported; SQL-level `DECLARE CURSOR` is not yet.
- **Advisory locks**: Not implemented.
- **VACUUM/ANALYZE**: Accepted as no-ops.

## Testing

```bash
# Run PG compatibility tests
cargo test pg_

# Test corpus
psql -h 127.0.0.1 -p 5432 -f tests/compat/pg_corpus.sql
```

## Compatibility targets

| Tool/Framework | Status | Notes |
|----------------|--------|-------|
| psql | Target | Primary CLI tool |
| pgAdmin | Target | GUI administration |
| DBeaver | Target | Universal database tool |
| Django | Target | `django.db.backends.postgresql` |
| Rails | Target | `activerecord-postgresql-adapter` |
| SQLAlchemy | Target | `postgresql://` dialect |
| psycopg2 | Target | Python driver |
| node-postgres | Target | Node.js driver |
| JDBC PostgreSQL | Target | Java driver |
