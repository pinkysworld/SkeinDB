# Merge Functions (Optimistic Concurrency) - Prototype

Status: Draft
Last updated: 2026-01-19

This document describes the prototype merge-function support for optimistic concurrency (R07).

## 1) Overview

Merge functions resolve write conflicts by combining a "current" row with an "incoming" row.
SkeinDB exposes:

- `merge.register` to store per-table merge policies
- `merge.apply` to apply a merge (with optional ETag conflict detection)
- `merge.simulate` to test merges without writes

Merge policies are stored on disk in `merge_policies.json` (format v1).

## 2) Policy format

A policy has a default merge function and optional per-column overrides:

```json
{
  "default": {"kind":"builtin","name":"last_write_wins"},
  "per_column": {"count": {"kind":"builtin","name":"sum"}}
}
```

Only `kind: "builtin"` is supported in this prototype.
Wasm-based merge functions are not implemented yet.

## 3) Built-in merge functions

- `last_write_wins` / `replace`: use incoming value
- `max`: numeric max
- `min`: numeric min
- `sum`: numeric sum (cross-type -> f64)
- `concat`: string concat or JSON array concat
- `set_union`: JSON array union (unique values)
- `object_merge`: JSON object merge (incoming overwrites)
- `reject`: keep current value

If a function cannot operate on the value types, SkeinDB falls back to the incoming value.

## 4) Conflict detection

`merge.apply` accepts `expected_etag`:
- If it matches the current row ETag, the merge is applied normally.
- If it does not match, `conflict=true` is returned and `conflicts=["write_write"]`.
  The merge still applies unless the policy is `reject` (default with no per-column overrides).

`merge.apply` also accepts `min_causality` (etag_chain_v1 token):
- If the dependency versions are not satisfied, `conflict=true`, `applied=false`,
  and `conflicts=["dependency"]` are returned.

Constraint conflicts:
- If the incoming row provides primary key values that do not match `pk`,
  `conflict=true`, `applied=false`, and `conflicts=["constraint"]` are returned.
- If the merged row would violate non-nullable columns, `conflict=true`,
  `applied=false`, and `conflicts=["constraint"]` are returned.

## 5) Limitations

- No Wasm execution yet (only built-ins).
- No cross-row merges or multi-row conflict resolution.
- Primary key values are immutable; mismatches are rejected.

## 6) Wasm merge registry (prototype)

Wasm merge functions are registered separately and referenced by module id.
The registry enforces a **values-only** capability model (no table access).

SkeinQL methods:
- `merge.wasm.register` — register a Wasm module + capabilities
- `merge.wasm.list` — list registered modules
- `merge.wasm.drop` — remove a module

Wasm execution is still disabled in this prototype; registration allows policies
to reference module ids in preparation for sandboxed execution.
Registry metadata is persisted in `merge_wasm_registry.json` (format v1).

## 7) Offline write queues

For offline-first clients, use the queue format in `docs/OFFLINE_WRITE_QUEUE.md`
to batch `merge.apply` operations and replay them on reconnect.
