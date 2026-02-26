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
  - `SELECT` (including literal-only and simple table queries)
  - `SHOW` (`DATABASES`, `TABLES`, `COLUMNS`)
  - `USE`
  - `CREATE DATABASE`, `CREATE TABLE`, `DROP TABLE`
  - `INSERT`, `UPDATE`, `DELETE`
  - `INSERT ... ON DUPLICATE KEY UPDATE` (corpus-oriented emulation)
  - `SQL_CALC_FOUND_ROWS` and `FOUND_ROWS()`
- The primary working interface in the scaffold is **SkeinQL JSON-RPC over HTTP**.
- The SQL story is split:
  - **SkeinQL** includes a full query/expression layer intended to cover common SQL patterns.
  - A planned **SQL→SkeinQL translation layer** will provide MySQL-ish SQL parsing and mapping.

If you want “drop-in MySQL for real apps”, the next concrete milestones are:
1) tighten MySQL semantic parity for edge cases and unsupported SHOW variants
2) broaden SQL and function compatibility with stricter parity tests
3) improve prepared-statement and optimizer parity for production drivers

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
