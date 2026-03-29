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
  - `SELECT` (literal-only, single-table, and simple `INNER JOIN` / `LEFT JOIN` / `RIGHT JOIN` / `CROSS JOIN` / `NATURAL JOIN` / `FULL OUTER JOIN` queries, including basic left-associative multi-join chains, top-level comma-separated `FROM` lists, baseline `JOIN ... USING (...)` rewrites for simple base-table joins with explicit/qualified projections, derived tables / FROM subqueries (`SELECT * FROM (SELECT ...) AS alias`), common table expressions (`WITH name AS (SELECT ...) SELECT * FROM name`), projection aliases with or without `AS`, and wildcard projections such as `SELECT *` / `table.*` / `db.table.*` over those supported join shapes)
  - `SHOW` (`DATABASES`, `TABLES`, `COLUMNS`)
  - `USE`
  - `CREATE DATABASE`, `CREATE TABLE`, `CREATE [UNIQUE] INDEX`, `DROP INDEX`, `DROP TABLE`
  - `ALTER TABLE ... ADD COLUMN` (including compatibility handling for `AFTER` / `FIRST` position clauses), `ALTER TABLE ... MODIFY [COLUMN]`, `ALTER TABLE ... CHANGE [COLUMN]`, `ALTER TABLE ... RENAME COLUMN`, `ALTER TABLE ... RENAME [KEY|INDEX] ... TO ...`, `ALTER TABLE ... RENAME [TO|AS] [db.]new_table`, `ALTER TABLE ... DROP COLUMN`, `ALTER TABLE ... ADD [UNIQUE] KEY`, and `ALTER TABLE ... DROP [KEY|INDEX]`
  - `INSERT`, `INSERT IGNORE`, `REPLACE`, `UPDATE`, `DELETE`
  - `INSERT ... SELECT`
  - `INSERT ... ON DUPLICATE KEY UPDATE` (declared key-aware compatibility routing)
  - Multi-table `DELETE`: `DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...`
  - Multi-table `UPDATE`: `UPDATE t1 JOIN t2 ON ... SET t1.col = ... WHERE ...` (executes via SELECT + affected-row counting)
  - `TRUNCATE TABLE` (rewrite to `DELETE FROM`)
  - `DROP DATABASE` / `DROP SCHEMA` (with `IF EXISTS`)
  - `RENAME TABLE ... TO ...` (rewrite to `ALTER TABLE RENAME`)
  - `CREATE VIEW` / `DROP VIEW` (no-op stubs)
  - `UNION` / `UNION ALL`
  - `SQL_CALC_FOUND_ROWS` and `FOUND_ROWS()`
- The MySQL wire layer also now includes a **basic prepared-statement baseline**:
  - `COM_STMT_PREPARE`, `COM_STMT_EXECUTE`, and `COM_STMT_CLOSE`
  - `COM_STMT_SEND_LONG_DATA`, `COM_STMT_RESET`, and baseline `COM_STMT_FETCH`
  - `?` placeholders are rebound into the same SQL-translation path as `COM_QUERY`
  - simple prepared `SELECT`s now advertise prepare-time result column counts and MySQL-style column definitions (including single-table `SELECT *`, simple join projections across the supported `INNER` / `LEFT` / `RIGHT` / `CROSS` join subset and top-level comma-separated `FROM` lists, baseline `JOIN ... USING (...)` explicit-projection queries over simple base-table joins, projection aliases with or without `AS`, supported scalar-expression projections such as arithmetic expressions, broader scalar/date-time functions including `FIND_IN_SET` / `ISNULL`, `DATE_FORMAT` / `FROM_UNIXTIME`, `DATEDIFF` / `TIMESTAMPDIFF`, `WEEKDAY` / `DAYOFWEEK` / `DAYOFYEAR`, `MONTHNAME` / `DAYNAME`, `QUARTER`, `LAST_DAY`, `EXTRACT(<unit> FROM ...)`, and baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` / `TIMESTAMPADD`, supported subquery-compat `SELECT`s whose `WHERE` clauses are rewritten through the compatibility layer, including the current `IN` / `EXISTS` / simple scalar-compare subset, plus `CASE` / `CAST`, and simple aggregate / grouped-aggregate compatibility queries)
  - prepared wildcard projections over the supported join subset now execute through the same wildcard-expansion path used by `COM_QUERY`, so prepare-time wildcard metadata and execute-time rows stay aligned for `SELECT *` / `table.*` / `db.table.*` join shapes
  - prepared `SELECT` responses are returned over the binary row protocol
  - read-only server-side cursor execution now works for prepared result sets
  - deeper prepare-time metadata parity (more complex joins/subqueries, richer exact types, stricter cursor/driver semantics) remains follow-on work
- The MySQL wire layer also now includes wire protocol support for `COM_INIT_DB` (0x02) and `COM_STATISTICS` (0x09).
- The MySQL wire layer also ships **compatibility shims** for the checked-in corpus in `tests/compat/corpus.sql`, including:
  - `BETWEEN` / `NOT BETWEEN` in `WHERE` clauses
  - `REGEXP` / `RLIKE` / `NOT REGEXP` pattern-matching operator
  - `<=>` (NULL-safe equality operator)
  - `SELECT ... FOR UPDATE` / `FOR SHARE` / `LOCK IN SHARE MODE` (locking hints stripped for compatibility)
  - `SELECT VERSION()` and `SELECT DATABASE()`
  - WordPress-style/bootstrap session queries such as `SET NAMES`, `SET CHARACTER SET`, `SET SESSION sql_mode`, `SET SQL_AUTO_IS_NULL`, `SET SESSION sql_notes`, `SET time_zone`, transaction-isolation/read-only `SET` forms, and `SELECT @@sql_mode` / `@@transaction_isolation` / `@@sql_auto_is_null`
  - literal session-variable selects with MySQL-style `LIMIT` forms (`LIMIT n`, `LIMIT offset,n`, `LIMIT n OFFSET offset`) for bootstrap probes such as `SELECT @@version_comment`
  - `SHOW FULL TABLES`, `SHOW TABLE STATUS`, `SHOW [FULL] COLUMNS`, `SHOW INDEX`, `SHOW CREATE TABLE`
  - `SHOW CREATE DATABASE`
  - `SHOW WARNINGS` / `SHOW ERRORS` (empty result compat)
  - `SHOW PROCESSLIST` / `SHOW FULL PROCESSLIST` (single-row stub)
  - `SHOW TRIGGERS` / `SHOW EVENTS` / `SHOW PROCEDURE STATUS` / `SHOW FUNCTION STATUS` (empty result stubs)
  - `SHOW PLUGINS` (single-row SkeinDB stub)
  - `SHOW PROFILES` (empty result stub)
  - `SHOW ENGINES` / `SHOW STORAGE ENGINES` (SkeinDB engine row)
  - `DESCRIBE` / `SHOW KEYS`
  - aggregate result emulation for `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`, `GROUP_CONCAT()`, `BIT_AND()`, `BIT_OR()`, and `BIT_XOR()` on both single-result aggregate queries, single-column `GROUP BY` queries (including compatibility-level single-result `HAVING` without `GROUP BY`, grouped `HAVING` for simple alias/aggregate-expression top-level `AND` predicates, and compatibility-level `ORDER BY` / `LIMIT` / `OFFSET` handling), and **multi-column `GROUP BY`** queries with multiple group columns and multiple aggregate expressions
  - **window functions**: `ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()` with `OVER(PARTITION BY ... ORDER BY ... [DESC])` clauses in `SELECT` projections
  - **user variables**: `SET @varname = value` and `SELECT @varname` with per-session state
  - compatibility rewrite for WordPress-style non-aggregate `GROUP BY` de-dup queries when grouped columns map to the full projected column set, including wildcard projections after schema expansion, simple `HAVING` predicates over grouped projected columns / aliases, and `SQL_CALC_FOUND_ROWS` / `FOUND_ROWS()` flows
  - wildcard projection execution for supported join shapes, including `SELECT *`, qualified `table.*` / `db.table.*`, and `SQL_CALC_FOUND_ROWS` / `FOUND_ROWS()` flows over those wildcard join result sets
  - explicit `CROSS JOIN` and top-level comma-separated `FROM` lists for common compatibility-style join queries
  - `NATURAL JOIN` for automatic column-name-based join conditions
  - `FULL OUTER JOIN` with full execution support (not just parsed)
  - derived tables / FROM subqueries (`SELECT * FROM (SELECT ...) AS alias`)
  - common table expressions (CTEs): `WITH name AS (SELECT ...) SELECT * FROM name`
  - MySQL-style column `DEFAULT` handling for `CREATE TABLE` / `ALTER TABLE ... ADD COLUMN`, including `SHOW FULL COLUMNS` / `SHOW CREATE TABLE` output
  - `KEY` / `UNIQUE KEY` metadata from MySQL DDL (including `ALTER TABLE ... ADD [UNIQUE] KEY` and `ALTER TABLE ... RENAME [KEY|INDEX]`), surfaced through `SHOW INDEX` / `SHOW CREATE TABLE`
  - `DISTINCT`, `IN (...)` / `NOT IN (...)`, `LIKE` / `NOT LIKE`, `IS NULL` / `IS NOT NULL`, and parenthesized `AND` / `OR` predicate trees for common WordPress query shapes, with `NULL` values now treated as SQL-style unknowns in comparison / `IN` / `LIKE` predicates
  - broader MySQL scalar-function coverage for `LOWER` / `UPPER`, `LENGTH` / `CHAR_LENGTH`, `TRIM` / `LTRIM` / `RTRIM`, `LEFT` / `RIGHT`, `SUBSTRING` / `SUBSTR`, `REPLACE`, `NULLIF`, `IF`, `LOCATE`, `INSTR`, `FIND_IN_SET`, `ISNULL`, `ABS`, `ROUND`, `FLOOR`, `CEIL` / `CEILING`, `MOD`, `LEAST`, `GREATEST`, `COALESCE`, `IFNULL`, `CONCAT`, `CONCAT_WS`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `SPACE`, `HEX`, `UNHEX`, `FORMAT`, `SIGN`, `SQRT`, `POW` / `POWER`, `TRUNCATE`, `LOG` / `LN`, `LOG2`, `LOG10`, `EXP`, `PI`, `RAND`, `UUID`, `SLEEP`, `BENCHMARK`, `FIELD`, `ELT`, `INET_ATON`, `INET_NTOA`, `BIN`, `OCT`, `CONV`, `CRC32`, `MD5`, `SHA1` / `SHA`, `SHA2`, `INSERT` (string), `MAKE_SET`, `EXPORT_SET`, `QUOTE`, `SUBSTRING_INDEX`, `ASCII`, `ORD`, `CHAR`, `STRCMP`, `BIT_LENGTH`, `OCTET_LENGTH`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `TO_BASE64`, `FROM_BASE64`, `DEGREES`, `RADIANS`, `PERIOD_ADD`, `PERIOD_DIFF`, `MAKEDATE`, and `MAKETIME` in translated projections and simple predicates
  - JSON function coverage: `JSON_EXTRACT`, `JSON_UNQUOTE`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_CONTAINS`, `JSON_LENGTH`, `JSON_TYPE`, `JSON_VALID`, `JSON_SET`, `JSON_KEYS`, `JSON_MERGE_PRESERVE`, `JSON_REMOVE`, `JSON_REPLACE`, and `JSON_INSERT`
  - baseline translated date/time scalar functions for `DATE`, `YEAR`, `MONTH`, `DAY` / `DAYOFMONTH`, `WEEKDAY`, `DAYOFWEEK`, `DAYOFYEAR`, `MONTHNAME`, `DAYNAME`, `QUARTER`, `LAST_DAY`, `EXTRACT(<unit> FROM ...)`, `HOUR`, `MINUTE`, `SECOND`, `UNIX_TIMESTAMP`, `DATE_FORMAT`, `FROM_UNIXTIME`, `DATEDIFF`, `TIMESTAMPDIFF`, baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` (with `INTERVAL <expr> <unit>` syntax) and `TIMESTAMPADD`, `NOW` / `CURRENT_TIMESTAMP` / `LOCALTIMESTAMP`, `CURDATE` / `CURRENT_DATE` / `CURTIME` / `CURRENT_TIME` / `LOCALTIME`, `STR_TO_DATE`, `WEEK`, `YEARWEEK`, `CONVERT_TZ`, `UTC_TIMESTAMP`, `UTC_DATE`, `UTC_TIME`, `SYSDATE`, `ADDTIME`, `SUBTIME`, `TIME_TO_SEC`, and `SEC_TO_TIME`
  - baseline translated `CASE ... WHEN ... THEN ... ELSE ... END` and `CAST(... AS ...)` expression support in projections, simple predicates, and scalar-expression `ORDER BY` clauses for the current translated subset
  - baseline translated arithmetic expression support for `+`, `-`, `*`, `/`, and `%` in projections, simple predicates, and scalar-expression `ORDER BY` clauses, including numeric ordering/filtering patterns such as `col + 0`
  - secondary-index-backed duplicate-key enforcement for `PRIMARY KEY` / `UNIQUE KEY` writes (including `PRIMARY KEY`-changing `UPDATE`s), declared PK / `UNIQUE KEY` conflict routing for `REPLACE` and `ON DUPLICATE KEY UPDATE`, duplicate-row rejection when creating a MySQL compatibility `UNIQUE INDEX` over existing data, durable per-table secondary-index cache metadata (`tables/<db>/<table>.sidx.json`) that reloads on reopen, and MySQL-style duplicate-key wire errors (`1062` / `23000`)
  - `SHOW VARIABLES`, `SHOW STATUS`, `SHOW CHARACTER SET`, `SHOW COLLATION`, `SHOW ENGINES`, `SHOW GRANTS` (including compatibility values for WordPress/common bootstrap variables such as `sql_auto_is_null`, charset/collation variables, `time_zone`, and `transaction_isolation`; unfiltered and scoped forms like `SHOW [SESSION|GLOBAL] VARIABLES`; simple `WHERE Variable_name ...` / `WHERE Charset ...` filters; plus wildcard patterns like `SHOW VARIABLES LIKE 'character_set_%'`)
  - `information_schema.schemata` virtual table
  - `information_schema.tables` virtual table with real table metadata
  - `information_schema.columns` virtual table with column metadata, ordinal positions, nullable/PK info
  - `information_schema.statistics` virtual table with real index data from PK + secondary indexes
  - `information_schema.key_column_usage` virtual table (PK + UNIQUE key columns)
  - `information_schema.table_constraints` virtual table (PK + UNIQUE constraints)
  - `information_schema.character_sets` virtual table
  - `information_schema.collations` virtual table
  - `information_schema.engines` virtual table
  - `information_schema.routines` virtual table (empty stub)
  - `information_schema.triggers` virtual table (empty stub)
  - `information_schema.views` virtual table (empty stub)
  - `information_schema.processlist` virtual table (single-row stub)
  - `information_schema.user_privileges` virtual table (single-row stub)
  - limited subquery compatibility rewrites for common adoption paths: `... WHERE <col> [NOT] IN (SELECT ...)`, `... WHERE [NOT] EXISTS (SELECT ...)`, and simple scalar comparison predicates such as `... WHERE <expr> = (SELECT ...)`, including parenthesized top-level `AND` / `OR` boolean trees that mix one or more of those predicates with translated non-subquery filters, negated `NOT (...)` wrappers when the resulting boolean tree can be pushed back into the translated comparison subset, recursive execution when nested inner subqueries also fit the current compatibility path, plus simple correlated rewrites for base-table subqueries whose outer references are top-level equality clauses (including equality-based correlated `IN` and multi-column `EXISTS` membership rewrites; scalar subqueries currently require a single projected column and at most one row)
  - `USER()` / `CURRENT_USER()` / `SESSION_USER()` / `SYSTEM_USER()` session functions
  - `LAST_INSERT_ID()` function with session tracking
  - `CONNECTION_ID()` function
  - `EXPLAIN` with real table name extraction from inner query (SELECT/UPDATE/DELETE/INSERT)
  - `DO` statement
  - `SAVEPOINT` / `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT` (no-op stubs)
  - `SET GLOBAL` forms (no-op compat)
  - `SET autocommit` (including qualified/session forms), `BEGIN`, `COMMIT`, `ROLLBACK`, and `START TRANSACTION`
  - `FLUSH` / `ANALYZE` / `OPTIMIZE` / `CHECK` / `REPAIR TABLE` (no-op compat)
  - `KILL` command (no-op)
  - compatibility no-op handling for `LOCK TABLES` / `UNLOCK TABLES`
- `crates/skeindb/tests/cluster_rpc.rs` now executes the entire compatibility corpus end-to-end over the MySQL port, so the corpus is enforced as a runtime baseline instead of only documented, including scalar-function, arithmetic-expression, extended date/time-function/formatting coverage (now including `DATEDIFF` / `TIMESTAMPDIFF`, `WEEKDAY` / `DAYOFWEEK` / `DAYOFYEAR`, `MONTHNAME` / `DAYNAME`, `QUARTER`, `LAST_DAY`, `EXTRACT(<unit> FROM ...)`, plus baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` / `TIMESTAMPADD`), single-result aggregate `HAVING` without `GROUP BY`, grouped-aggregate `HAVING`, non-aggregate wildcard `GROUP BY` de-dup coverage, `CASE` / `CAST`, expression-ordering, projection aliases with or without `AS`, wildcard join projection coverage (`SELECT *`, qualified `table.*`, and schema-qualified `db.table.*`), explicit `CROSS JOIN`, comma-list join coverage, baseline `JOIN ... USING (...)` explicit-projection coverage, parenthesized boolean-tree correlated/nested-subquery coverage, simple scalar-subquery comparison coverage, simple correlated `EXISTS` coverage, duplicate-check coverage for creating MySQL compatibility unique indexes, `BETWEEN` / `NOT BETWEEN`, `COUNT(DISTINCT col)`, `GROUP_CONCAT()`, `INSERT ... SELECT`, `UNION` / `UNION ALL`, `TRUNCATE TABLE`, `DROP DATABASE`, `RENAME TABLE`, session functions (`USER()`, `LAST_INSERT_ID()`, `CONNECTION_ID()`), `EXPLAIN` stub, `DO` statement, `SAVEPOINT` stubs, locking hint stripping, `information_schema.schemata` / `information_schema.statistics`, expanded `SHOW` commands, and the full set of additional scalar and date/time functions listed above, plus derived tables (FROM subqueries), CTEs (`WITH...AS`), `REGEXP`/`RLIKE`/`NOT REGEXP`, `<=>` (NULL-safe equality), `NATURAL JOIN`, `FULL OUTER JOIN`, multi-table `DELETE`, multi-table `UPDATE` (executed), 14 JSON functions (`JSON_EXTRACT`/`JSON_UNQUOTE`/`JSON_OBJECT`/`JSON_ARRAY`/`JSON_CONTAINS`/`JSON_LENGTH`/`JSON_TYPE`/`JSON_VALID`/`JSON_SET`/`JSON_KEYS`/`JSON_MERGE_PRESERVE`/`JSON_REMOVE`/`JSON_REPLACE`/`JSON_INSERT`), additional scalar functions (`FIELD`/`ELT`, `INET_ATON`/`INET_NTOA`, `BIN`/`OCT`/`CONV`, `CRC32`, `MD5`, `SHA1`/`SHA`, `SHA2`, `INSERT`/`MAKE_SET`/`EXPORT_SET`/`QUOTE`), 9 `information_schema` virtual tables (`tables`/`columns`/`schemata`/`statistics`/`key_column_usage`/`table_constraints`/`character_sets`/`collations`/`engines`), `SHOW ENGINES`, `GROUP_CONCAT` with `SEPARATOR`/`DISTINCT`/`ORDER BY` stripping, and `EXPLAIN` with real table extraction. The corpus has expanded from 1081 lines to 1130 lines with over 374 SQL statements.
- The primary working interface in the scaffold is **SkeinQL JSON-RPC over HTTP**.
- The SQL story is split:
  - **SkeinQL** includes a full query/expression layer intended to cover common SQL patterns.
  - A shipped but intentionally narrow **SQL→SkeinQL translation layer** now provides the current MySQL-ish subset; broader parity work is still ongoing.

If you want “drop-in MySQL for real apps”, the next concrete milestones are:
1) broaden `COM_QUERY` parity beyond the current WordPress-class baseline with stricter parity tests (deeper correlated/nested subqueries beyond the current recursive `IN` / `EXISTS` / simple scalar-compare compatibility path, broader function/date/time/cast parity beyond the current scalar + date/time baseline, and broader `ALTER TABLE` variants beyond the current `ADD/MODIFY/CHANGE/RENAME COLUMN/RENAME [KEY|INDEX]/RENAME TO/DROP COLUMN` plus index metadata surface)
2) deepen prepared-statement parity (complex-query metadata, stricter driver/cursor semantics, fuller protocol coverage) and optimizer parity for production drivers, even though prepare-time metadata now also covers supported scalar-expression projections including baseline arithmetic, broader scalar/date-time functions including `FIND_IN_SET` / `ISNULL`, `DATE_FORMAT` / `FROM_UNIXTIME`, `DATEDIFF` / `TIMESTAMPDIFF`, `WEEKDAY` / `DAYOFWEEK` / `DAYOFYEAR`, `MONTHNAME` / `DAYNAME`, `QUARTER`, `LAST_DAY`, `EXTRACT(<unit> FROM ...)`, and baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` / `TIMESTAMPADD`, supported subquery-compat `SELECT`s whose `WHERE` clauses rewrite cleanly, including the current `IN` / `EXISTS` / simple scalar-compare subset, plus `CASE` / `CAST` and simple aggregate / grouped-aggregate compatibility shims

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
