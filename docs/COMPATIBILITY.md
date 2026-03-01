# SkeinDB Compatibility (MySQL / SQL)

Status: Draft v0.1
Last updated: 2026-02-28

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
- `UNIQUE KEY` / `KEY` clauses are preserved in compatibility metadata and surfaced through MySQL-style metadata queries
- `UNIQUE KEY` semantics are enforced for inserts/updates, but the current implementation is scan-based rather than backed by a true secondary index structure
- ALTER TABLE `ADD COLUMN` (including MySQL-style `DEFAULT`)
- DROP TABLE

### DML
- INSERT / INSERT IGNORE / REPLACE
- INSERT ... ON DUPLICATE KEY UPDATE
- `INSERT IGNORE` still keeps a small leading-column fast path, but `REPLACE` and `ON DUPLICATE KEY UPDATE` now resolve duplicate-key behavior through declared PK / `UNIQUE KEY` metadata; the implementation remains scan-based rather than backed by a true secondary index structure
- UPDATE/DELETE with simple WHERE
- SELECT with WHERE / ORDER BY / LIMIT / OFFSET
- SELECT supports `DISTINCT`, `IN (...)`, `LIKE`, `IS NULL`, `IS NOT NULL`
- INNER JOIN and LEFT JOIN (simple single-join shapes); RIGHT/FULL joins are not implemented yet
- GROUP BY + aggregates remain mostly open beyond the current `COUNT(*)` compatibility shim
- SQL_CALC_FOUND_ROWS + FOUND_ROWS()

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
and pagination/count queries.

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
