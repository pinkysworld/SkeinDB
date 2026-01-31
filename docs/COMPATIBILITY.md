# SkeinDB Compatibility (MySQL / SQL)

Status: Draft v0.1
Last updated: 2026-01-17

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

## 2) v0.1 SQL surface (target)

### DDL
- CREATE DATABASE / DROP DATABASE / USE
- CREATE TABLE (PK, UNIQUE, KEY)
- ALTER TABLE (add/drop column, add/drop index)
- DROP TABLE

### DML
- INSERT / INSERT IGNORE
- INSERT ... ON DUPLICATE KEY UPDATE
- UPDATE/DELETE with WHERE (optional LIMIT)
- SELECT with WHERE / ORDER BY / LIMIT
- INNER JOIN + LEFT JOIN
- GROUP BY + aggregates
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
- tables, columns, statistics, engines, user_privileges

---

## 3) Differential testing

The file `tests/compat/corpus.sql` is the primary compatibility driver.
Add queries there first, then implement.

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
