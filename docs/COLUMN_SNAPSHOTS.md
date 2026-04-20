# Hybrid Row + Column Snapshots (HTAP-lite)

Status: Prototype in progress
Last updated: 2026-04-20

Goal:
Accelerate analytic queries (scans, aggregates) without sacrificing OLTP writes.

SkeinDB remains row/MVCC first. Column snapshots are optional, read-optimized, and built asynchronously from a stable MVCC snapshot.

---

## 1) Terminology

- Row store: MVCC row versions (primary truth)
- Column snapshot: read-only columnar representation of a subset of rows at a snapshot_ts

---

## 2) Snapshot creation

A snapshot build chooses:
- db + table
- optional projected columns
- snapshot_ts

Process:
1) scan table rows and filter them through the current `row_visible_at(...)` semantics
2) normalize `snapshot_ts` from micros to commit-ts millis when needed
3) materialize primary-key columns plus projected values for rows visible at `snapshot_ts`
4) persist the in-memory snapshot cache to `snapshots.json`
5) write `manifest.json` plus one `col-XXXX.cseg` file per stored column under `data/snapshots/`

Snapshots are immutable.

Prototype note:
- The current implementation is consistent with SkeinDB's existing `AS OF` visibility behavior.
- It does not yet reconstruct overwritten historical row versions beyond what the current row store retains.

---

## 3) File layout

data/
  snapshots.json
  snapshots/
    snap-<snapshot_id>/
      manifest.json
      col-0001.cseg
      col-0002.cseg

Current prototype:
- `snapshots.json` at the data dir root stores snapshot metadata + row values used by the runtime.
- Each snapshot also gets a best-effort sidecar directory with `manifest.json` and `.cseg` files.
- Snapshot directories are rebuilt on persist and removed when the snapshot disappears from `snapshots.json`.

Manifest fields:
- `format_version`
- `id`, `db`, `table`
- `columns`, `pk_columns`
- `snapshot_ts`, `table_version`, `row_count`
- `segments[]` with `column`, `is_pk`, `file`, `row_count`, `non_null_count`, `encoding`

---

## 4) Column segment format (cseg v1)

A column segment stores a column for a row range.

Header:
- magic = `SKNCSEG1`
- format_ver = `u32` (current: `1`)
- kind = `u8` (`0` = value column, `1` = primary-key column)
- snapshot_ts = `u64`
- row_count = `u64`
- db name (VarU length + UTF-8 bytes)
- table name (VarU length + UTF-8 bytes)
- column name (VarU length + UTF-8 bytes)
- null bitmap (VarU length + bytes)

Body:
- non-null values are stored in row order as `VarU length + JSON-encoded Lit payload`
- nulls are reconstructed from the null bitmap
- manifest entries reserve `min_value` / `max_value` for future statistics, but the current writer leaves them unset

Implementation note:
The current writer uses `plain+null_bitmap` only. Dictionary, RLE, and richer stats remain future work.

---

## 5) Query planning

Planner chooses:
- row scan for hot data not covered by snapshots
- column scan for covered ranges when query is:
  - read-only
  - aggregation-heavy
  - uses a subset of columns available in snapshot

Hybrid plan:
- scan column snapshots for cold partitions
- scan row store for newest partitions
- merge results

---

## 6) Consistency

A column snapshot is consistent at snapshot_ts.

Queries at current time may combine:
- cold snapshot portion (at snapshot_ts)
- hot row portion (at current snapshot)

To avoid anomalies, hybrid queries should run at a chosen snapshot_ts and treat:
- snapshots built at <= snapshot_ts are eligible
- newer changes are read from row store and merged

---

## 7) Maintenance and refresh

Policies:
- build snapshots periodically (nightly)
- build snapshots when a partition becomes cold
- rebuild snapshots after major compactions

Current prototype:
- Incremental refresh applies inserts/updates/deletes to in-memory snapshots.
- Refresh also rewrites the snapshot `manifest.json` and `.cseg` sidecars.
- Snapshots are invalidated if primary key data is missing.

---

## 8) Metrics

Expose:
- snapshot_build_time
- snapshot_bytes
- snapshot_query_hit_rate
- snapshot_rows_covered

---

## Research extension: Adaptive row-column hybrid execution

The baseline column snapshot design is an **explicit** HTAP-lite capability.
The 2026 research agenda proposes making snapshot materialization **adaptive** based on observed query patterns.
See: `docs/research_agenda/R02_adaptive-row-column-hybrid-execution.md`.

Key adaptation points:
- Add a cost model: snapshot build cost vs projected query savings.
- Observe hot projections (frequently used column subsets) via query fingerprints.
- Extend dependency tracking to column granularity so snapshots can be incrementally refreshed or invalidated.
- Implement an online controller that decides **when** to create/refresh snapshots within configured resource budgets.

Prototype (scaffold status):
- Cost model, pattern tracking, and online controller are implemented for single-table SELECTs.
- Snapshots persist in `snapshots.json`, emit sidecar `.cseg` artifacts, and are loaded best-effort on startup when `table_version` still matches.
- Snapshot reader planning/execution remains follow-up work in T101/T102.
