# SkeinDB Compatibility (MySQL / PostgreSQL / SQL)

Status: v0.3.9 truth sync
Last updated: 2026-05-09

SkeinDB adoption strategy:
- Speak MySQL wire protocol (port 3306) so existing MySQL apps work unchanged.
- Speak PostgreSQL wire protocol (port 5432, partial baseline today) so early PG clients can connect while broader dialect/catalog parity is completed.
- Translate MySQL / the current PostgreSQL subset into SkeinIR.
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
- CREATE DATABASE / DROP DATABASE / DROP SCHEMA (with IF EXISTS) / USE
- CREATE TABLE (column defs, PK, `AUTO_INCREMENT`, column `DEFAULT`)
- CREATE INDEX / CREATE UNIQUE INDEX
- DROP INDEX
- TRUNCATE TABLE (rewrite to DELETE FROM)
- RENAME TABLE ... TO ... (rewrite to ALTER TABLE RENAME)
- CREATE VIEW / DROP VIEW (no-op stubs)
- `UNIQUE KEY` / `KEY` clauses are preserved in compatibility metadata and surfaced through MySQL-style metadata queries
- `UNIQUE KEY` semantics are enforced for inserts/updates through in-memory compatibility key indexes, creating a MySQL compatibility `UNIQUE INDEX` now rejects pre-existing duplicate rows, and MySQL compatibility `KEY` / `UNIQUE KEY` metadata now seeds and is best-effort restored into the prototype's in-memory secondary-index prefilter path on reopen; the current implementation is still not backed by a durable reusable secondary index structure
- ALTER TABLE `ADD COLUMN` / `MODIFY COLUMN` / `CHANGE COLUMN` / `RENAME COLUMN` / `RENAME [TO|AS] [db.]new_table` / `DROP COLUMN` (including MySQL-style `DEFAULT` and compatibility handling for `AFTER` / `FIRST` position clauses)
- ALTER TABLE `ADD KEY` / `ADD UNIQUE KEY` (compatibility metadata updates reflected in `SHOW INDEX` / `SHOW CREATE TABLE`)
- DROP TABLE

### DML
- INSERT / INSERT IGNORE / REPLACE
- INSERT ... SELECT
- INSERT ... ON DUPLICATE KEY UPDATE
- `INSERT IGNORE` still keeps a small leading-column fast path, but `REPLACE` and `ON DUPLICATE KEY UPDATE` now resolve duplicate-key behavior through declared PK / `UNIQUE KEY` metadata backed by in-memory compatibility key indexes; durable reusable secondary-index parity is still open
- UPDATE/DELETE with simple WHERE
- Multi-table DELETE: `DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...`
- Multi-table UPDATE (executed): `UPDATE t1 JOIN t2 ON ... SET t1.col = ... WHERE ...`
- SELECT with WHERE / ORDER BY / LIMIT / OFFSET (including scalar-expression `ORDER BY` for the translated compatibility subset)
- SELECT supports `DISTINCT`, `IN (...)` / `NOT IN (...)`, `LIKE` / `NOT LIKE`, `IS NULL`, `IS NOT NULL`, `BETWEEN` / `NOT BETWEEN`, `REGEXP` / `RLIKE` / `NOT REGEXP`, `<=>` (NULL-safe equality), and parenthesized `AND` / `OR` boolean filter trees
- SELECT ... FOR UPDATE / FOR SHARE / LOCK IN SHARE MODE (locking hints stripped for compatibility)
- UNION / UNION ALL
- Comparison / `IN` / `LIKE` predicates now treat `NULL` as SQL-style unknown rather than matching like an ordinary value
- MySQL-style scalar functions now include baseline `LOWER` / `UPPER`, `LENGTH` / `CHAR_LENGTH`, `TRIM` / `LTRIM` / `RTRIM`, `LEFT` / `RIGHT`, `SUBSTRING` / `SUBSTR`, `REPLACE`, `NULLIF`, `IF`, `LOCATE`, `INSTR`, `FIND_IN_SET`, `ISNULL`, `ABS`, `ROUND`, `FLOOR`, `CEIL` / `CEILING`, `MOD`, `LEAST`, `GREATEST`, `COALESCE`, `IFNULL`, `CONCAT`, `CONCAT_WS`, `REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `SPACE`, `HEX`, `UNHEX`, `FORMAT`, `SIGN`, `SQRT`, `POW` / `POWER`, `TRUNCATE`, `LOG` / `LN`, `LOG2`, `LOG10`, `EXP`, `PI`, `RAND`, `UUID`, `SLEEP`, `BENCHMARK`, `FIELD`, `ELT`, `INET_ATON`, `INET_NTOA`, `BIN`, `OCT`, `CONV`, `CRC32`, `MD5`, `SHA1` / `SHA`, `SHA2`, `INSERT` (string), `MAKE_SET`, `EXPORT_SET`, `QUOTE`, `SUBSTRING_INDEX`, `ASCII`, `ORD`, `CHAR`, `STRCMP`, `BIT_LENGTH`, `OCTET_LENGTH`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `TO_BASE64`, and `FROM_BASE64` in translated projections and simple predicates
- JSON function coverage: `JSON_EXTRACT`, `JSON_UNQUOTE`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_CONTAINS`, `JSON_LENGTH`, `JSON_TYPE`, `JSON_VALID`, `JSON_SET`, `JSON_KEYS`, `JSON_MERGE_PRESERVE`, `JSON_REMOVE`, `JSON_REPLACE`, and `JSON_INSERT`
- Session/system functions: `USER()` / `CURRENT_USER()` / `SESSION_USER()` / `SYSTEM_USER()`, `LAST_INSERT_ID()` (with session tracking), `CONNECTION_ID()`
- Translated scalar expressions now also include baseline `CASE ... WHEN ... THEN ... ELSE ... END` and `CAST(... AS ...)` support in projections, simple predicates, and scalar-expression `ORDER BY` clauses
- Translated arithmetic expressions now also include baseline `+`, `-`, `*`, `/`, and `%` support in projections, simple predicates, and scalar-expression `ORDER BY` clauses, including common numeric ordering/filtering patterns such as `col + 0`
- Translated date/time scalar functions now also include baseline `DATE`, `YEAR`, `MONTH`, `DAY` / `DAYOFMONTH`, `WEEKDAY`, `DAYOFWEEK`, `DAYOFYEAR`, `MONTHNAME`, `DAYNAME`, `QUARTER`, `LAST_DAY`, `EXTRACT(<unit> FROM ...)`, `HOUR`, `MINUTE`, `SECOND`, `UNIX_TIMESTAMP`, `DATE_FORMAT`, `FROM_UNIXTIME`, `DATEDIFF`, `TIMESTAMPDIFF`, baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` (with `INTERVAL <expr> <unit>` syntax) and `TIMESTAMPADD`, `NOW` / `CURRENT_TIMESTAMP` / `LOCALTIMESTAMP`, `CURDATE` / `CURRENT_DATE` / `CURTIME` / `CURRENT_TIME` / `LOCALTIME`, `STR_TO_DATE`, `WEEK`, `YEARWEEK`, `CONVERT_TZ`, `UTC_TIMESTAMP`, `UTC_DATE`, `UTC_TIME`, `SYSDATE`, `ADDTIME`, `SUBTIME`, `TIME_TO_SEC`, and `SEC_TO_TIME`
- INNER JOIN, LEFT JOIN, RIGHT JOIN, NATURAL JOIN, and FULL OUTER JOIN (single-join and basic left-associative multi-join chains)
- Derived tables / FROM subqueries: `SELECT * FROM (SELECT ...) AS alias`
- Common table expressions (CTEs): `WITH name AS (SELECT ...) SELECT * FROM name`
- GROUP BY + full aggregate semantics remain mostly open, but compatibility shims now cover simple single-result and single-column grouped `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`, and `GROUP_CONCAT()` queries (including baseline grouped `HAVING` for simple alias/aggregate-expression top-level `AND` predicates plus basic grouped `ORDER BY` / `LIMIT` / `OFFSET`), plus **multi-column `GROUP BY`** queries with multiple group columns and multiple aggregate expressions
- Non-aggregate `GROUP BY` compatibility now includes WordPress-style de-dup queries when grouped columns match the full projected column set (rewritten through the `DISTINCT` path, including `SQL_CALC_FOUND_ROWS` flows)
- SQL_CALC_FOUND_ROWS + FOUND_ROWS()
- Compatibility subquery rewrites now cover parenthesized top-level `AND` / `OR` boolean trees that mix translated predicates with `IN (SELECT ...)` / `[NOT] EXISTS (SELECT ...)` and simple scalar comparison predicates such as `col = (SELECT ...)`, negated `NOT (...)` wrappers when the resulting boolean tree can still be pushed into the translated comparison subset, recursive execution when nested inner subqueries also fit the current compatibility path, plus simple equality-based correlated rewrites for base-table subqueries, including correlated `IN` and multi-column `EXISTS` membership cases; scalar-subquery comparisons currently require one projected column and at most one row, and broader correlated/nested forms still remain open
- `EXPLAIN` with real table name extraction from inner SELECT/UPDATE/DELETE/INSERT queries
- `DO` statement
- `SAVEPOINT` / `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT` (no-op stubs)
- `SET GLOBAL` forms (no-op compat)
- `START TRANSACTION`, `BEGIN`, `COMMIT`, `ROLLBACK`
- `FLUSH` / `ANALYZE` / `OPTIMIZE` / `CHECK` / `REPAIR TABLE` (no-op compat)
- `KILL` command (no-op)

### MySQL wire protocol
- Handshake + `mysql_native_password`
- COM_QUERY over the current SQL-translation subset
- `COM_INIT_DB` (0x02) wire protocol command
- `COM_STATISTICS` (0x09) wire protocol command
- Basic `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` / `COM_STMT_CLOSE`
- `COM_STMT_SEND_LONG_DATA` + `COM_STMT_RESET` + baseline `COM_STMT_FETCH`
- Simple prepared `SELECT`s now return prepare-time result column definitions (including single-table `SELECT *`, simple join projections, supported scalar-expression projections such as arithmetic expressions, broader scalar/date-time functions including `FIND_IN_SET` / `ISNULL`, `DATE_FORMAT` / `FROM_UNIXTIME`, `DATEDIFF` / `TIMESTAMPDIFF`, `WEEKDAY` / `DAYOFWEEK` / `DAYOFYEAR`, `MONTHNAME` / `DAYNAME`, `QUARTER`, `LAST_DAY`, `EXTRACT(<unit> FROM ...)`, and baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` / `TIMESTAMPADD`, supported subquery-compat `SELECT`s whose `WHERE` clauses rewrite cleanly, including the current `IN` / `EXISTS` / simple scalar-compare subset, plus `CASE` / `CAST`, and simple aggregate / grouped-aggregate compatibility queries), and prepared result rows are returned in the binary row protocol
- Read-only prepared cursor execution now works for result sets; broader prepare metadata parity for more complex queries and stricter driver behavior is still open

### SHOW / metadata
- SHOW DATABASES / TABLES / FULL TABLES
- SHOW TABLE STATUS
- SHOW [FULL] COLUMNS
- SHOW INDEX
- SHOW CREATE TABLE
- SHOW CREATE DATABASE
- SHOW VARIABLES / STATUS
- SHOW ENGINES / SHOW STORAGE ENGINES
- SHOW GRANTS
- SHOW WARNINGS / SHOW ERRORS (empty result compat)
- SHOW PROCESSLIST / SHOW FULL PROCESSLIST (single-row stub)
- SHOW TRIGGERS / SHOW EVENTS / SHOW PROCEDURE STATUS / SHOW FUNCTION STATUS (empty result stubs)
- SHOW PLUGINS (single-row SkeinDB stub)
- SHOW PROFILES (empty result stub)

### INFORMATION_SCHEMA
- `tables` with real table metadata (catalog, schema, name, type, engine, rows)
- `columns` with column metadata (ordinal position, nullable, data type, column key)
- `schemata`
- `statistics` with real index data from primary keys and secondary indexes
- `key_column_usage` (primary key + unique key columns)
- `table_constraints` (primary key + unique constraints)
- `character_sets` (utf8mb4, utf8, latin1, binary)
- `collations` (6 standard collations)
- `engines` (SkeinDB engine row)
- `routines` (empty stub)
- `triggers` (empty stub)
- `views` (empty stub)
- `processlist` (single-row stub)
- `user_privileges` (single-row stub)

---

## 3) Differential testing

The file `tests/compat/corpus.sql` is the primary compatibility driver.
Add queries there first, then implement.

The MySQL integration suite now executes that corpus end-to-end over the wire listener,
so the checked-in corpus is the enforced baseline for compatibility work.
The PostgreSQL baseline now also has a dedicated live-wire corpus at `tests/compat/pg_corpus.sql`
with a matching integration test, so PG startup probes, shared-engine SQL, and savepoint behavior
have a checked-in regression driver too.
That corpus now includes WordPress-style bootstrap, metadata, duplicate-key, default-value,
pagination/count, grouped aggregate compatibility, projection-grouped `GROUP BY` de-dup + `FOUND_ROWS`,
parenthesized `AND` / `OR` filter queries, broader MySQL scalar-function coverage, baseline arithmetic
expression coverage, extended date/time-function/formatting coverage including `DATEDIFF` / `TIMESTAMPDIFF`,
`WEEKDAY` / `DAYOFWEEK` / `DAYOFYEAR`, `MONTHNAME` / `DAYNAME`, `QUARTER`, `LAST_DAY`, `EXTRACT(<unit> FROM ...)`, plus baseline interval arithmetic through `DATE_ADD` / `DATE_SUB` / `TIMESTAMPADD`, grouped-aggregate
`HAVING` coverage, plus `CASE` / `CAST` expression coverage including scalar-expression `ORDER BY`,
`ALTER TABLE ... RENAME COLUMN`, `ALTER TABLE ... RENAME [TO|AS]`, parenthesized boolean-tree
subquery rewrite coverage, baseline nested subquery compatibility coverage, simple scalar-subquery
comparison coverage, simple correlated `EXISTS` coverage, duplicate-check coverage for creating
MySQL compatibility unique indexes, `BETWEEN` / `NOT BETWEEN`, `COUNT(DISTINCT col)`, `GROUP_CONCAT()`,
`INSERT ... SELECT`, `UNION` / `UNION ALL`, `TRUNCATE TABLE`, `DROP DATABASE`, `RENAME TABLE`,
session functions (`USER()`, `LAST_INSERT_ID()`, `CONNECTION_ID()`), `EXPLAIN` stub, `DO` statement,
`SAVEPOINT` stubs, locking hint stripping, `information_schema.schemata` / `information_schema.statistics`,
additional `SHOW` commands (WARNINGS, ERRORS, PROCESSLIST, TRIGGERS, EVENTS, PROCEDURE STATUS,
FUNCTION STATUS, PLUGINS, PROFILES, CREATE DATABASE), additional scalar functions (`CONCAT_WS`,
`REPEAT`, `REVERSE`, `LPAD`, `RPAD`, `SPACE`, `HEX`, `UNHEX`, `FORMAT`, `SIGN`, `SQRT`, `POW`,
`TRUNCATE`, `LOG`/`LN`, `LOG2`, `LOG10`, `EXP`, `PI`, `RAND`, `UUID`, `SLEEP`, `BENCHMARK`),
additional date/time functions (`STR_TO_DATE`, `WEEK`, `YEARWEEK`, `CONVERT_TZ`, `UTC_TIMESTAMP`,
`UTC_DATE`, `UTC_TIME`, `SYSDATE`, `ADDTIME`, `SUBTIME`, `TIME_TO_SEC`, `SEC_TO_TIME`), wire
protocol additions (`COM_INIT_DB`, `COM_STATISTICS`), and no-op stubs for `CREATE VIEW`/`DROP VIEW`,
`FLUSH`/`ANALYZE`/`OPTIMIZE`/`CHECK`/`REPAIR TABLE`, `SET GLOBAL`, and `KILL`.
Recent additions include derived tables (FROM subqueries), common table expressions (CTEs),
`REGEXP`/`RLIKE`/`NOT REGEXP` pattern matching, `<=>` (NULL-safe equality), `NATURAL JOIN`,
`FULL OUTER JOIN`, multi-table `DELETE`, multi-table `UPDATE` execution, 11 JSON functions
(`JSON_EXTRACT`, `JSON_UNQUOTE`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_CONTAINS`, `JSON_LENGTH`,
`JSON_TYPE`, `JSON_VALID`, `JSON_SET`, `JSON_KEYS`, `JSON_MERGE_PRESERVE`), plus additional
scalar functions (`FIELD`, `ELT`, `INET_ATON`/`INET_NTOA`, `BIN`/`OCT`, `CONV`, `CRC32`,
`MD5`, `SHA1`/`SHA`, `SHA2`), plus `INSERT` (string), `MAKE_SET`, `EXPORT_SET`, `QUOTE`,
`JSON_REMOVE`, `JSON_REPLACE`, `JSON_INSERT`, 9 `information_schema` virtual tables
(`tables`/`columns`/`schemata`/`statistics`/`key_column_usage`/`table_constraints`/
`character_sets`/`collations`/`engines`), `SHOW ENGINES`, `GROUP_CONCAT` with `SEPARATOR` stripping,
and `EXPLAIN` with real table name extraction.
The corpus has expanded to 1657 lines with about 678 semicolon-terminated SQL statements, and dedicated MySQL-wire regressions now also cover WordPress installer seed queries (escaped single-row `INSERT`s, trailing `/* ... */` comments, serialized option upserts, and `information_schema.TABLES ... IN (...) AND ENGINE = ...` probes), WordPress Users-screen role-count aggregates built from `COUNT(NULLIF(<predicate>, false))`, and Site Health storage-summary queries. A fresh live WordPress admin sweep across Dashboard, Posts, Pages, Media, Comments, Themes, Site Editor, Plugins, Users, Tools, Site Health, and Settings now completes with an empty `debug.log`; the only remaining 500s in that smoke are the theme-owned `nav-menus` / `widgets` pages.

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

---

## 6) PostgreSQL compatibility (current partial baseline)

SkeinDB now ships a PostgreSQL v3 wire protocol listener on a separate port (default 5432).
The implementation is still partial but substantially deeper than the first bring-up baseline.

### Current baseline
- **Wire protocol:** PostgreSQL v3 frontend/backend framing, StartupMessage + SSLRequest parsing, and backend message encoding for `ParameterStatus`, `BackendKeyData`, `ReadyForQuery`, `RowDescription`, `DataRow`, `CommandComplete`, and `ErrorResponse`
- **Authentication:** trust mode when `SKEINDB_TOKEN` is unset; SCRAM-SHA-256 when it is set
- **Simple query flow:** shared SQL execution engine behind the PG socket, including `SELECT 1`-style queries, a `SELECT version()` compatibility response, and typed `RowDescription` metadata for the current `BOOL` / `INT8` / `FLOAT8` / `TEXT` / `DATE` / `TIME` / `TIMESTAMP` / `JSONB` / `BYTEA` / `UUID` baseline when the shared engine exposes those schema or literal types
- **Virtual catalogs:** shared-executor `pg_catalog` coverage for `pg_database`, `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_index`, `pg_constraint`, `pg_proc` (stub), `pg_settings`, and `pg_stat_activity`
- **PG dialect and DML:** `::` casts, dollar quoting, double-quoted identifiers, `IS [NOT] DISTINCT FROM`, `FETCH FIRST`, array constructors, common `ON CONFLICT` rewrites, supported `RETURNING` extraction, and explicit unsupported-feature errors for `COPY`
- **Transaction handling:** PostgreSQL-style `ReadyForQuery` state (`I` / `T` / `E`), failed-transaction-block behavior, and undo-log-backed `SAVEPOINT` / `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT`
- **Extended query protocol:** `Parse` / `Bind` / `Describe` / `Execute` / `Sync` / `Close` / `Flush`, named statements + portals, `$1` placeholders, sync-based recovery after extended-query errors, typed `RowDescription` metadata, and binary result encoding for the current common scalar OID baseline
- **PG regression corpus:** dedicated `tests/compat/pg_corpus.sql` executed end-to-end over the live PG listener

### Architecture
Both MySQL and PostgreSQL frontends parse SQL into the shared `SqlPlan` / SkeinQL IR layer, so the execution engine is fully protocol-agnostic.

```
MySQL (3306) ──┐
PG    (5432) ──┤──→ SqlPlan / SkeinQL IR ──→ Engine
HTTP  (8080) ──┘
```

### Configuration
- `--pg <port>` CLI flag (0 = disabled)
- `pg_port` in `skeindb-config.json`

### Still open
- broad PostgreSQL dialect parity beyond the current rewrite layer and corpus-backed subset
- COPY protocol
- partial portal suspension for incremental `Execute` row draining
- broader type/array/domain encoding beyond the current scalar and array OID baseline
- broader driver/framework coverage and framework-specific bootstrap suites

See `docs/PG_COMPAT.md` for the tested scope, examples, and backlog map.
