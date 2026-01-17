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
