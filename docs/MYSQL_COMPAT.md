# MySQL Compatibility

SkeinDB is designed to be *adoptable* by existing applications that already speak MySQL.

The **compatibility layer** is intentionally treated as an *adoption surface*:
- it makes it possible to point existing apps/tools at SkeinDB with minimal change
- while SkeinDB retains a separate, research-friendly control plane (SkeinQL)

This document explains the intended scope and the current scaffold status.

---

## What “MySQL compatibility” means here

There are two related layers:

### 1) MySQL protocol compatibility
Support the MySQL client/server wire protocol so standard clients and drivers can connect.

Examples:
- command-line clients (`mysql`)
- application drivers (JDBC, ODBC, Node mysql2, PHP mysqli, ...)
- tooling (migration tools, admin GUIs)

### 2) SQL dialect compatibility
Support a subset of MySQL SQL syntax and semantics.

Even with protocol support, SQL dialect mismatches can break apps.

---

## Current status in this repository

- The CLI `--mysql` listener now supports a **minimal MySQL wire handshake** with `mysql_native_password` auth exchange.
- The listener supports a `COM_QUERY` translation subset through `sql.exec` for:
  - `SELECT` (literal-only, single-table, and simple `INNER JOIN` / `LEFT JOIN` / `RIGHT JOIN` queries, including basic left-associative multi-join chains)
  - `SHOW` (`DATABASES`, `TABLES`, `COLUMNS`)
  - `USE`
  - `CREATE DATABASE`, `CREATE TABLE`, `CREATE [UNIQUE] INDEX`, `DROP INDEX`, `DROP TABLE`
  - `ALTER TABLE ... ADD COLUMN` (including compatibility handling for `AFTER` / `FIRST` position clauses), `ALTER TABLE ... MODIFY [COLUMN]`, `ALTER TABLE ... CHANGE [COLUMN]`, `ALTER TABLE ... RENAME COLUMN`, `ALTER TABLE ... DROP COLUMN`, `ALTER TABLE ... ADD [UNIQUE] KEY`, and `ALTER TABLE ... DROP [KEY|INDEX]`
  - `INSERT`, `INSERT IGNORE`, `REPLACE`, `UPDATE`, `DELETE`
  - `INSERT ... ON DUPLICATE KEY UPDATE` (declared key-aware compatibility routing)
  - `SQL_CALC_FOUND_ROWS` and `FOUND_ROWS()`
- The MySQL wire layer also now includes a **basic prepared-statement baseline**:
  - `COM_STMT_PREPARE`, `COM_STMT_EXECUTE`, and `COM_STMT_CLOSE`
  - `COM_STMT_SEND_LONG_DATA`, `COM_STMT_RESET`, and baseline `COM_STMT_FETCH`
  - `?` placeholders are rebound into the same SQL-translation path as `COM_QUERY`
  - simple prepared `SELECT`s now advertise prepare-time result column counts and MySQL-style column definitions (including single-table `SELECT *`, simple join projections, supported scalar-expression projections, and simple aggregate / grouped-aggregate compatibility queries)
  - prepared `SELECT` responses are returned over the binary row protocol
  - read-only server-side cursor execution now works for prepared result sets
  - deeper prepare-time metadata parity (more complex joins/subqueries, richer exact types, stricter cursor/driver semantics) remains follow-on work
- The MySQL wire layer also ships **compatibility shims** for the checked-in corpus in `tests/compat/corpus.sql`, including:
  - `SELECT VERSION()` and `SELECT DATABASE()`
  - WordPress-style/bootstrap session queries such as `SET NAMES`, `SET CHARACTER SET`, `SET SESSION sql_mode`, `SET SQL_AUTO_IS_NULL`, `SET SESSION sql_notes`, `SET time_zone`, transaction-isolation/read-only `SET` forms, and `SELECT @@sql_mode` / `@@transaction_isolation` / `@@sql_auto_is_null`
  - literal session-variable selects with MySQL-style `LIMIT` forms (`LIMIT n`, `LIMIT offset,n`, `LIMIT n OFFSET offset`) for bootstrap probes such as `SELECT @@version_comment`
  - `SHOW FULL TABLES`, `SHOW TABLE STATUS`, `SHOW [FULL] COLUMNS`, `SHOW INDEX`, `SHOW CREATE TABLE`
  - `DESCRIBE` / `SHOW KEYS`
  - aggregate result emulation for `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, and `AVG(col)` on both single-result aggregate queries and simple single-column `GROUP BY` queries (with compatibility-level `ORDER BY` / `LIMIT` / `OFFSET` handling)
  - compatibility rewrite for WordPress-style non-aggregate `GROUP BY` de-dup queries when grouped columns map to the full projected column set (including `SQL_CALC_FOUND_ROWS` / `FOUND_ROWS()` flows)
  - MySQL-style column `DEFAULT` handling for `CREATE TABLE` / `ALTER TABLE ... ADD COLUMN`, including `SHOW FULL COLUMNS` / `SHOW CREATE TABLE` output
  - `KEY` / `UNIQUE KEY` metadata from MySQL DDL (including `ALTER TABLE ... ADD [UNIQUE] KEY`), surfaced through `SHOW INDEX` / `SHOW CREATE TABLE`
  - `DISTINCT`, `IN (...)` / `NOT IN (...)`, `LIKE` / `NOT LIKE`, `IS NULL` / `IS NOT NULL`, and parenthesized `AND` / `OR` predicate trees for common WordPress query shapes, with `NULL` values now treated as SQL-style unknowns in comparison / `IN` / `LIKE` predicates
  - broader MySQL scalar-function coverage for `LOWER` / `UPPER`, `LENGTH` / `CHAR_LENGTH`, `TRIM` / `LTRIM` / `RTRIM`, `LEFT` / `RIGHT`, `SUBSTRING` / `SUBSTR`, `REPLACE`, `NULLIF`, `IF`, `LOCATE`, `INSTR`, `ABS`, `ROUND`, `FLOOR`, `CEIL` / `CEILING`, `MOD`, `LEAST`, `GREATEST`, `COALESCE`, `IFNULL`, and `CONCAT` in translated projections and simple predicates
  - index-backed in-memory `UNIQUE KEY` probe enforcement for inserts/updates, declared PK / `UNIQUE KEY` conflict routing for `REPLACE` and `ON DUPLICATE KEY UPDATE`, duplicate-row rejection when creating a MySQL compatibility `UNIQUE INDEX` over existing data, and best-effort registration of MySQL compatibility `KEY` / `UNIQUE KEY` definitions into the same in-memory secondary-index prefilter path used by the prototype index advisor
  - `SHOW VARIABLES`, `SHOW STATUS`, `SHOW CHARACTER SET`, `SHOW COLLATION`, `SHOW ENGINES`, `SHOW GRANTS` (including compatibility values for WordPress/common bootstrap variables such as `sql_auto_is_null`, charset/collation variables, `time_zone`, and `transaction_isolation`; unfiltered and scoped forms like `SHOW [SESSION|GLOBAL] VARIABLES`; simple `WHERE Variable_name ...` / `WHERE Charset ...` filters; plus wildcard patterns like `SHOW VARIABLES LIKE 'character_set_%'`)
  - limited subquery compatibility rewrites for common adoption paths: `... WHERE <col> [NOT] IN (SELECT ...)` and `... WHERE [NOT] EXISTS (SELECT ...)`, including top-level `AND` chains that mix one or more of those predicates with translated non-subquery filters plus simple correlated rewrites for base-table subqueries whose outer references are top-level equality clauses (including equality-based correlated `IN` and multi-column `EXISTS` membership rewrites)
  - `SET autocommit` (including qualified/session forms), `BEGIN`, `COMMIT`, and `ROLLBACK` for the corpus' insert/rollback flow
  - compatibility no-op handling for `LOCK TABLES` / `UNLOCK TABLES`
- `crates/skeindb/tests/cluster_rpc.rs` now executes the entire compatibility corpus end-to-end over the MySQL port, so the corpus is enforced as a runtime baseline instead of only documented.
- The primary working interface in the scaffold is **SkeinQL JSON-RPC over HTTP**.
- The SQL story is split:
  - **SkeinQL** includes a full query/expression layer intended to cover common SQL patterns.
  - A shipped but intentionally narrow **SQL→SkeinQL translation layer** now provides the current MySQL-ish subset; broader parity work is still ongoing.

If you want “drop-in MySQL for real apps”, the next concrete milestones are:
1) complete duplicate-key enforcement hardening from the current in-memory probe indexes to full durable/reusable secondary-index-backed unique-key semantics
2) broaden SQL and function compatibility with stricter parity tests beyond the bundled corpus (deeper correlated/nested subqueries beyond the current simple `EXISTS` rewrite, broader function coverage beyond the current scalar baseline, and broader `ALTER TABLE` variants beyond the current `ADD/MODIFY/CHANGE/RENAME/DROP COLUMN` plus index metadata surface)
3) deepen prepared-statement parity (complex-query metadata, stricter driver/cursor semantics, fuller protocol coverage) and optimizer parity for production drivers, even though prepare-time metadata now also covers supported scalar-expression projections plus simple aggregate / grouped-aggregate compatibility shims

---

## Compatibility telemetry and migration hints

A central research idea in SkeinDB is to *instrument* the compatibility layer:

- track which SQL features a workload actually uses
- emit **automatic migration hints** (MySQL → SkeinQL)
- provide “compatibility coverage” metrics (what percentage of a real workload would run)

See:
- `docs/COMPAT_TELEMETRY.md`
- the paper section “Compatibility telemetry + migration hints”

---

## Suggested implementation plan

A staged plan that keeps SkeinDB single-binary:

1) **Protocol skeleton**
   - handle handshake + capability flags
   - accept a connection and respond to a fixed `SELECT 1`

2) **Minimal SQL subset**
   - implement `SELECT ... FROM table WHERE pk = ?`
   - implement parameter binding

3) **Translation to SkeinQL**
   - parse SQL into an AST
   - translate to SkeinQL `Query` + `Expr`

4) **Coverage + correctness**
   - expand supported functions, joins, aggregation
   - add golden tests (same query → same result) against MySQL

5) **Performance work**
   - prepared statement caching
   - vectorized execution path for common scans
