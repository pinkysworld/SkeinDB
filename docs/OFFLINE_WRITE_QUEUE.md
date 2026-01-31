# Offline Write Queue (merge.apply batches)

Status: Draft
Last updated: 2026-01-22

This document defines a client-side format for batching optimistic writes while
offline, then replaying them using `merge.apply` when connectivity returns.

## 1) Format (offline_queue_v1)

Top-level schema:

```json
{
  "format": "offline_queue_v1",
  "client_id": "client-123",
  "created_at_ms": 1730000000000,
  "min_causality": {"format":"etag_chain_v1","deps":[{"table":"app.counters","v":12}]},
  "items": [ ... ]
}
```

Item schema:

```json
{
  "op_id": "op-1",
  "table": {"db":"app","table":"counters"},
  "pk": [{"t":"u64","v":1}],
  "incoming": {"count":{"t":"u64","v":2}},
  "expected_etag": "W/\"r:...\"",
  "min_causality": {"format":"etag_chain_v1","deps":[{"table":"app.counters","v":12}]},
  "policy": {"default":{"kind":"builtin","name":"sum"}},
  "depends_on": ["op-0"]
}
```

Notes:
- `min_causality` at the queue level is inherited by items unless overridden.
- `depends_on` expresses client-local ordering constraints.
- `policy` is optional; most clients rely on server-registered merge policies.

## 2) Replay protocol

1) Sort items by dependency graph (`depends_on`).
2) For each item:
   - call `merge.apply`
   - record the result and update local state

## 3) Merge result handling

`merge.apply` returns `applied`, `conflict`, `conflicts[]`, and `merged`.

Recommended handling:

- **applied=true, conflict=false**
  - Mark item as applied.
  - Update local row to `merged`.

- **applied=true, conflict=true**
  - Mark item as applied-with-conflict.
  - Update local row to `merged`.
  - Surface conflict metadata (`conflicts`) to the app if needed.

- **applied=false, conflict=true**
  - If `conflicts` includes `dependency`, retry after refreshing causal state.
  - If `conflicts` includes `constraint`, surface to user / error out.
  - If `conflicts` includes `write_write`, rebase against server row and retry.

## 4) Conflict kinds

Current conflict kinds:
- `write_write` — ETag mismatch (concurrent update)
- `dependency` — min_causality not satisfied
- `constraint` — PK mismatch or non-null violation

## 5) Security

Queues are client-controlled. Servers must still enforce:
- primary key immutability
- non-null constraints (where applicable)
- Wasm capability restrictions (values-only)

Queue formats are advisory and do not grant elevated privileges.
