# Incremental Views (Prototype)

Status: Draft
Last updated: 2026-04-25

This document describes the prototype incremental view maintenance for R08.

## 1) Overview

SkeinDB can materialize simple views and refresh them incrementally using the change log.

Supported methods:
- `view.create`
- `view.drop`
- `view.refresh`
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

## 4) Dependencies

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
