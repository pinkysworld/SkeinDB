# SkeinDB Compatibility (MySQL / SQL / Postgres)

Status: Draft v0.1
Last updated: 2026-03-26

SkeinDB adoption strategy:
- Speak MySQL wire protocol so existing apps work unchanged.
- Translate MySQL SQL into SkeinIR.
- Expose bounded Postgres-style SQL shims through `sql.exec` for browser/admin adoption paths.
- Provide SkeinQL native API + console for proprietary features.

---

## 1) Compatibility levels

### compat=mysql8-strict
- Match MySQL behaviors as closely as possible.

### compat=mysql8-default
- Match mainstream behavior for common apps.

### compat=skein-native
- Best performance + extra features.

### compat=postgres-adapter
- Postgres-style SQL rewrites over `sql.exec` for admin/HTTP tooling, without a Postgres wire listener.

---

## 2) v0.1 SQL surface (current baseline)

### DDL
- CREATE DATABASE / DROP DATABASE / USE
- Postgres-style `CREATE SCHEMA` / `DROP SCHEMA` rewrite to SkeinDB namespaces through `sql.exec`
- CREATE TABLE (column defs, PK, `AUTO_INCREMENT`, column `DEFAULT`)
- `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` map to auto-incrementing integer columns
- Inline column `PRIMARY KEY` / `UNIQUE` declarations are now recognized in `CREATE TABLE`
- CREATE INDEX / CREATE UNIQUE INDEX
- `CREATE INDEX IF NOT EXISTS`
- DROP INDEX
- `UNIQUE KEY` / `KEY` clauses are preserved in compatibility metadata and surfaced through MySQL-style metadata queries
- `UNIQUE KEY` semantics are enforced for inserts/updates, but the current implementation is scan-based rather than backed by a true secondary index structure
- ALTER TABLE `ADD COLUMN` (including MySQL-style `DEFAULT` and compatibility handling for `AFTER` / `FIRST` position clauses)
- ALTER TABLE `ADD KEY` / `ADD UNIQUE KEY` / `RENAME INDEX` (compatibility metadata updates reflected in `SHOW INDEX` / `SHOW CREATE TABLE`)
- DROP TABLE

### DML
- INSERT / INSERT IGNORE / REPLACE
- INSERT ... ON DUPLICATE KEY UPDATE
- `INSERT IGNORE` still keeps a small leading-column fast path, but `REPLACE` and `ON DUPLICATE KEY UPDATE` now resolve duplicate-key behavior through declared PK / `UNIQUE KEY` metadata; the implementation remains scan-based rather than backed by a true secondary index structure
- UPDATE/DELETE with simple WHERE
- SELECT with WHERE / ORDER BY / LIMIT / OFFSET
- Postgres-style literal session helpers through `sql.exec`: `current_schema()`, `current_database()`, `version()`, and selected `current_setting(...)` values
- SELECT supports `DISTINCT`, `IN (...)` / `NOT IN (...)`, `LIKE` / `NOT LIKE`, `IS NULL`, `IS NOT NULL`, and parenthesized `AND` / `OR` boolean filter trees
- Comparison / `IN` / `LIKE` predicates now treat `NULL` as SQL-style unknown rather than matching like an ordinary value
- INNER JOIN, LEFT JOIN, and RIGHT JOIN (single-join and basic left-associative multi-join chains); FULL joins are not implemented yet
- Wildcard `SELECT *` and qualified wildcard projections like `table.*` now work across that supported join subset, including mixed projections such as `p.*, u.name` and `SQL_CALC_FOUND_ROWS` flows
- GROUP BY + full aggregate semantics remain mostly open, but compatibility shims now cover simple single-result and single-column grouped `COUNT(*)`, `COUNT(col)`, and `SUM(col)` queries (including basic aggregate `HAVING` filters plus grouped `ORDER BY` / `LIMIT` / `OFFSET`)
- Non-aggregate `GROUP BY` compatibility now includes WordPress-style de-dup queries when grouped columns match the full projected column set (rewritten through the `DISTINCT` path, including `SQL_CALC_FOUND_ROWS` flows)
- SQL_CALC_FOUND_ROWS + FOUND_ROWS()

### MySQL wire protocol
- Handshake + `mysql_native_password`
- COM_QUERY over the current SQL-translation subset
- Basic `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` / `COM_STMT_CLOSE`
- `COM_STMT_SEND_LONG_DATA` + `COM_STMT_RESET` + baseline `COM_STMT_FETCH`
- Simple prepared `SELECT`s now return prepare-time result column definitions (including single-table `SELECT *`, join wildcard projections, and simple join projections), and prepared result rows are returned in the binary row protocol
- Read-only prepared cursor execution now works for result sets; broader prepare metadata parity for more complex queries and stricter driver behavior is still open

### SHOW / metadata
- SHOW DATABASES / TABLES / FULL TABLES
- SHOW TABLE STATUS
- SHOW [FULL] COLUMNS
- SHOW INDEX
- SHOW CREATE TABLE
- SHOW VARIABLES / STATUS
- SHOW ENGINES
- SHOW GRANTS

### INFORMATION_SCHEMA
- `tables`, `columns`
- richer compatibility views such as `statistics`, `engines`, and `user_privileges` remain backlog work

### Postgres-oriented `sql.exec` mode
- No PostgreSQL wire listener is shipped yet.
- SkeinAdmin and other HTTP/RPC clients can opt into `dialect: "postgres"` on `sql.exec`.
- That mode currently rewrites `CREATE/DROP SCHEMA`, `SET/SHOW search_path`, and common session literal helpers onto the existing engine/namespace model.

---

## 3) Differential testing

The file `tests/compat/corpus.sql` is the primary compatibility driver.
Add queries there first, then implement.

The MySQL integration suite now executes that corpus end-to-end over the wire listener,
so the checked-in corpus is the enforced baseline for compatibility work.
That corpus now includes WordPress-style bootstrap, metadata, duplicate-key, default-value,
pagination/count, wildcard and qualified-wildcard join projections, grouped aggregate compatibility,
projection-grouped `GROUP BY` de-dup + `FOUND_ROWS`, and parenthesized `AND` / `OR` filter queries.

---

## 4) Compatibility telemetry (recommended)

SkeinDB should record which MySQL features are exercised by real applications.
This enables:
- prioritizing implementation work
- spotting deprecated patterns
- generating migration hints toward SkeinQL

See docs/TELEMETRY_AND_MIGRATION.md.

---

## 5) Compatibility-friendly extensions (opt-in)

SkeinDB must remain usable by stock MySQL clients. However, some proprietary features can be exposed
in ways that do not require new SQL grammar:

- Session variables:
  - `SET @@skein.as_of = '<iso_ts>'` (historical snapshot reads)
  - `SET @@skein.autoparameterize = 1` (normalized-plan reuse)

- Comment hints:
  - `SELECT /*+ SKEIN_AS_OF('2026-01-01T00:00:00Z') */ ...`

Notes:
- These are intentionally namespaced under `skein.*` and are disabled by default in strict compatibility mode.
