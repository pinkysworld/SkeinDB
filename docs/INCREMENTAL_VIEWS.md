# Incremental Views

Status: Hardened research surface (v0.3.15)
Last updated: 2026-05-11

This document describes the R08 incremental view maintenance surface.

## 1) Overview

SkeinDB can materialize simple views and refresh them incrementally using the change log.

Supported methods:
- `view.create`
- `view.drop`
- `view.refresh`
- `view.evaluate`
- `view.status`
- `view.explain_deps`

View state is stored in `views.json` (format v2).

## 2) Supported view definitions

Only a restricted SELECT form is supported:
- single-table `FROM` (no joins, no subqueries)
- optional `GROUP BY` over base-table columns only
- grouped projections may only contain grouped columns plus `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`
- no `HAVING` or `DISTINCT`
- no `ORDER BY` or `LIMIT`

The base table must have a primary key.

## 3) Incremental refresh

`view.refresh` supports:
- `mode: "full"`: recompute from scratch
- `mode: "incremental"`: apply deltas from the change log
- `mode: "auto"`: choose between incremental and full recompute based on changed primary keys, grouped cardinality, and stale state

Incremental refresh:
- re-evaluates the view predicate for each changed primary key
- upserts or removes view rows accordingly
- for grouped views, persists the contributing source rows in `views.json` and recomputes only the touched groups during incremental refresh

Views are marked `stale=true` when base tables change.
Reads may return stale results until the view is refreshed.

## 4) Evaluation oracle and benchmark report

`view.evaluate` is a read-only harness for a specific materialized view. It clones
the current view state, runs incremental refresh on one clone and full recompute
on another clone, compares the resulting row signatures, and reports:

- `format: "skein.view.evaluate.v1"`
- pending change count and touched primary-key count
- whether the incremental result matches full recompute
- mean incremental/full refresh nanoseconds over the requested iterations
- speedup versus full recompute
- the current auto-refresh recommendation (`incremental` or `full`)

The harness is intentionally deterministic and does not mutate the live view.
Focused tests cover a deterministic pseudo-random update workload plus the
JSON-RPC roundtrip used by SkeinAdmin.

## 5) Dependencies

`view.status` and `view.explain_deps` return one dependency object per base table.
Each object currently includes:

- `columns`: the full set of columns referenced anywhere in the view definition
- `projection_columns`: columns used by the view projection
- `predicate_columns`: columns used by the `WHERE` predicate
- `group_by_columns`: columns used by `GROUP BY` in grouped views

For the current single-table prototype, this metadata is derived from the same
query-analysis path used by other planner surfaces and then persisted alongside
the view entry, which keeps dependency tracking aligned with predicate and
projection extraction in the rest of the engine across restarts.

This metadata is intended for dependency tracking, future optimizer work, and
operator visibility in SkeinAdmin.

## 6) Compatibility catalogs

Materialized views now appear in virtual compatibility catalogs:

- MySQL `information_schema.tables` emits `TABLE_TYPE = 'VIEW'` rows.
- MySQL `information_schema.views` emits definition/updatability metadata.
- PostgreSQL `pg_catalog.pg_views` emits `schemaname`, `viewname`, `viewowner`, and `definition`.

These are adoption shims for ORMs and admin tools; SQL `CREATE VIEW` remains a
compatibility no-op while the native `view.create` method is the authoritative
runtime surface.
