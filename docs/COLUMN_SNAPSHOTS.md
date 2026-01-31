# Hybrid Row + Column Snapshots (HTAP-lite)

Status: Draft
Last updated: 2026-01-17

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
- table_id
- key range (or partition)
- snapshot_ts

Process:
1) scan rows in range
2) materialize visible version at snapshot_ts
3) encode columns into column chunks
4) write snapshot manifest + column files

Snapshots are immutable.

---

## 3) File layout

data/
  snapshots/
    snap-<table_id>-<snapshot_ts>/
      manifest.json
      col-0001.cseg
      col-0002.cseg

Prototype (scaffold):
- `snapshots.json` at the data dir root stores snapshot metadata + row values.
- Format version is tracked in the JSON (`format_version`).
- Column files (.cseg) remain future work.

---

## 4) Column segment format (cseg v0.1)

A column segment stores a column for a row range.

Header:
- magic
- format_ver
- table_id
- col_id
- snapshot_ts
- row_count
- min_value (optional)
- max_value (optional)

Body:
- encoding = one of:
  - plain (typed values)
  - dictionary (values + ids)
  - run-length (RLE)
  - bitmap for nulls

Implementation note:
A simple plain+null bitmap format is enough for v0.1.

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

Prototype (scaffold):
- Incremental refresh applies inserts/updates/deletes to in-memory snapshots.
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
- Snapshots persist in `snapshots.json` and are loaded best-effort on startup.
