# Incremental Views (Prototype)

Status: Draft
Last updated: 2026-01-19

This document describes the prototype incremental view maintenance for R08.

## 1) Overview

SkeinDB can materialize simple views and refresh them incrementally using the change log.

Supported methods:
- `view.create`
- `view.drop`
- `view.refresh`
- `view.status`
- `view.explain_deps`

View state is stored in `views.json` (format v1).

## 2) Supported view definitions

Only a restricted SELECT form is supported:
- single-table `FROM` (no joins, no subqueries)
- no `GROUP BY`, `HAVING`, `DISTINCT`
- no `ORDER BY` or `LIMIT`

The base table must have a primary key.

## 3) Incremental refresh

`view.refresh` supports:
- `mode: "full"`: recompute from scratch
- `mode: "incremental"`: apply deltas from the change log
- `mode: "auto"`: choose based on a simple cost heuristic

Incremental refresh:
- re-evaluates the view predicate for each changed primary key
- upserts or removes view rows accordingly

Views are marked `stale=true` when base tables change.
Reads may return stale results until the view is refreshed.

## 4) Dependencies

`view.explain_deps` returns the base table and the columns referenced by the view
projection and predicate. This metadata is used for dependency tracking and future
optimization work.
