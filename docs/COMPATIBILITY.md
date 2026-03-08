# SkeinDB Compatibility (MySQL / SQL)

Status: Draft v0.1
Last updated: 2026-03-08

SkeinDB adoption strategy:
- Speak MySQL wire protocol so existing apps work unchanged.
- Translate MySQL SQL into SkeinIR.
- Provide SkeinQL native API + console for proprietary features.

---

## 1) Compatibility levels

### compat=mysql8-strict
- Match MySQL behaviors as closely as possible.

### compat=mysql8-default
- Match mainstream behavior for common apps.

### compat=skein-native
- Best performance + extra features.

---

## 2) v0.1 SQL surface (current baseline)

### DDL
- CREATE DATABASE / DROP DATABASE / USE
- CREATE TABLE (column defs, PK, `AUTO_INCREMENT`, column `DEFAULT`)
- CREATE INDEX / CREATE UNIQUE INDEX
- DROP INDEX
- `UNIQUE KEY` / `KEY` clauses are preserved in compatibility metadata and surfaced through MySQL-style metadata queries
- `UNIQUE KEY` semantics are enforced for inserts/updates through in-memory compatibility key indexes, creating a MySQL compatibility `UNIQUE INDEX` now rejects pre-existing duplicate rows, and MySQL compatibility `KEY` / `UNIQUE KEY` metadata now seeds and is best-effort restored into the prototype's in-memory secondary-index prefilter path on reopen; the current implementation is still not backed by a durable reusable secondary index structure
- ALTER TABLE `ADD COLUMN` / `MODIFY COLUMN` / `CHANGE COLUMN` / `RENAME COLUMN` / `DROP COLUMN` (including MySQL-style `DEFAULT` and compatibility handling for `AFTER` / `FIRST` position clauses)
- ALTER TABLE `ADD KEY` / `ADD UNIQUE KEY` (compatibility metadata updates reflected in `SHOW INDEX` / `SHOW CREATE TABLE`)
- DROP TABLE

### DML
- INSERT / INSERT IGNORE / REPLACE
- INSERT ... ON DUPLICATE KEY UPDATE
- `INSERT IGNORE` still keeps a small leading-column fast path, but `REPLACE` and `ON DUPLICATE KEY UPDATE` now resolve duplicate-key behavior through declared PK / `UNIQUE KEY` metadata backed by in-memory compatibility key indexes; durable reusable secondary-index parity is still open
- UPDATE/DELETE with simple WHERE
- SELECT with WHERE / ORDER BY / LIMIT / OFFSET (including scalar-expression `ORDER BY` for the translated compatibility subset)
- SELECT supports `DISTINCT`, `IN (...)` / `NOT IN (...)`, `LIKE` / `NOT LIKE`, `IS NULL`, `IS NOT NULL`, and parenthesized `AND` / `OR` boolean filter trees
- Comparison / `IN` / `LIKE` predicates now treat `NULL` as SQL-style unknown rather than matching like an ordinary value
- MySQL-style scalar functions now include baseline `LOWER` / `UPPER`, `LENGTH` / `CHAR_LENGTH`, `TRIM` / `LTRIM` / `RTRIM`, `LEFT` / `RIGHT`, `SUBSTRING` / `SUBSTR`, `REPLACE`, `NULLIF`, `IF`, `LOCATE`, `INSTR`, `FIND_IN_SET`, `ISNULL`, `ABS`, `ROUND`, `FLOOR`, `CEIL` / `CEILING`, `MOD`, `LEAST`, `GREATEST`, `COALESCE`, `IFNULL`, and `CONCAT` in translated projections and simple predicates
- Translated scalar expressions now also include baseline `CASE ... WHEN ... THEN ... ELSE ... END` and `CAST(... AS ...)` support in projections, simple predicates, and scalar-expression `ORDER BY` clauses
- Translated arithmetic expressions now also include baseline `+`, `-`, `*`, `/`, and `%` support in projections, simple predicates, and scalar-expression `ORDER BY` clauses, including common numeric ordering/filtering patterns such as `col + 0`
- Translated date/time scalar functions now also include baseline `DATE`, `YEAR`, `MONTH`, `DAY` / `DAYOFMONTH`, `WEEKDAY`, `DAYOFWEEK`, `DAYOFYEAR`, `MONTHNAME`, `DAYNAME`, `HOUR`, `MINUTE`, `SECOND`, `UNIX_TIMESTAMP`, `DATE_FORMAT`, `FROM_UNIXTIME`, `DATEDIFF`, `TIMESTAMPDIFF`, baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` (with `INTERVAL <expr> <unit>` syntax) and `TIMESTAMPADD`, `NOW` / `CURRENT_TIMESTAMP` / `LOCALTIMESTAMP`, and `CURDATE` / `CURRENT_DATE` / `CURTIME` / `CURRENT_TIME` / `LOCALTIME`
- INNER JOIN, LEFT JOIN, and RIGHT JOIN (single-join and basic left-associative multi-join chains); FULL joins are not implemented yet
- GROUP BY + full aggregate semantics remain mostly open, but compatibility shims now cover simple single-result and single-column grouped `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, and `AVG(col)` queries (including baseline grouped `HAVING` for simple alias/aggregate-expression top-level `AND` predicates plus basic grouped `ORDER BY` / `LIMIT` / `OFFSET`)
- Non-aggregate `GROUP BY` compatibility now includes WordPress-style de-dup queries when grouped columns match the full projected column set (rewritten through the `DISTINCT` path, including `SQL_CALC_FOUND_ROWS` flows)
- SQL_CALC_FOUND_ROWS + FOUND_ROWS()
- Compatibility subquery rewrites now cover parenthesized top-level `AND` / `OR` boolean trees that mix translated predicates with `IN (SELECT ...)` / `[NOT] EXISTS (SELECT ...)`, recursive execution when nested inner subqueries also fit the current compatibility path, plus simple equality-based correlated rewrites for base-table subqueries, including correlated `IN` and multi-column `EXISTS` membership cases; broader correlated/nested forms still remain open

### MySQL wire protocol
- Handshake + `mysql_native_password`
- COM_QUERY over the current SQL-translation subset
- Basic `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` / `COM_STMT_CLOSE`
- `COM_STMT_SEND_LONG_DATA` + `COM_STMT_RESET` + baseline `COM_STMT_FETCH`
- Simple prepared `SELECT`s now return prepare-time result column definitions (including single-table `SELECT *`, simple join projections, supported scalar-expression projections such as arithmetic expressions, broader scalar/date-time functions including `FIND_IN_SET` / `ISNULL`, `DATE_FORMAT` / `FROM_UNIXTIME`, `DATEDIFF` / `TIMESTAMPDIFF`, `WEEKDAY` / `DAYOFWEEK` / `DAYOFYEAR`, `MONTHNAME` / `DAYNAME`, and baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` / `TIMESTAMPADD`, supported subquery-compat `SELECT`s whose `WHERE` clauses rewrite cleanly, plus `CASE` / `CAST`, and simple aggregate / grouped-aggregate compatibility queries), and prepared result rows are returned in the binary row protocol
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

---

## 3) Differential testing

The file `tests/compat/corpus.sql` is the primary compatibility driver.
Add queries there first, then implement.

The MySQL integration suite now executes that corpus end-to-end over the wire listener,
so the checked-in corpus is the enforced baseline for compatibility work.
That corpus now includes WordPress-style bootstrap, metadata, duplicate-key, default-value,
pagination/count, grouped aggregate compatibility, projection-grouped `GROUP BY` de-dup + `FOUND_ROWS`,
parenthesized `AND` / `OR` filter queries, broader MySQL scalar-function coverage, baseline arithmetic
expression coverage, extended date/time-function/formatting coverage including `DATEDIFF` / `TIMESTAMPDIFF`,
`WEEKDAY` / `DAYOFWEEK` / `DAYOFYEAR`, `MONTHNAME` / `DAYNAME`, plus baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` / `TIMESTAMPADD`, grouped-aggregate
`HAVING` coverage, plus `CASE` / `CAST` expression coverage including scalar-expression `ORDER BY`,
`ALTER TABLE ... RENAME COLUMN`, parenthesized boolean-tree subquery rewrite coverage, baseline nested
subquery compatibility coverage, simple correlated `EXISTS` coverage, and duplicate-check coverage for
creating MySQL compatibility unique indexes.

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
