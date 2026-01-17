# SkeinDB Project Backlog (Codex-friendly)

This backlog is designed for small PR-sized tasks.
Each task should include tests.

## Phase 0 — Repo setup
- [ ] T001: Encoding primitives (VarU, Bytes/String, CRC32C)
- [ ] T002: FileHeader read/write
- [ ] T003: RecordFrame append/iterate

## Phase 1 — Storage core
- [ ] T010: MANIFEST.log reader/writer
- [ ] T011: WAL writer/reader + recovery
- [ ] T012: ValueStore (.vseg) append/read + ValueID
- [ ] T013: Sorted runs (.run) + simple LSM (memtable + level0)
- [ ] T014: RowSeg (.rseg) + RowVersion encoding
- [ ] T015: RowDir (row_id -> head ptr)
- [ ] T016: MVCC visibility

## Phase 2 — SQL + virtual metadata
- [ ] T020: Catalog schema + TableDef
- [ ] T021: information_schema.tables + columns
- [ ] T022: Minimal executor: CREATE TABLE, INSERT, SELECT scan+filter+limit

## Phase 3 — MySQL protocol
- [ ] T030: Handshake + mysql_native_password
- [ ] T031: COM_QUERY SELECT literals
- [ ] T032: SQL translator (subset)
- [ ] T033: DDL/DML subset for corpus.sql
- [ ] T034: SQL_CALC_FOUND_ROWS + FOUND_ROWS

## Phase 4 — Web console
- [ ] T040: HTTP API `/api/v1/sql/exec`
- [ ] T041: Console UI scaffold
- [ ] T042: Schema browser + SQL editor
- [ ] T043: Data browse/edit + import/export
- [ ] T044: Users/privileges + status dashboard
