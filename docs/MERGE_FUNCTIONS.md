# Merge Functions (Optimistic Concurrency)

Status: Hardened research surface (v0.3.14)
Last updated: 2026-05-11

This document describes the R07 merge-function runtime for optimistic concurrency, offline-first clients, and CRDT-style conflict resolution.

SkeinDB exposes:

- `merge.register` to persist a per-table merge policy.
- `merge.apply` to merge an incoming row into the current row, optionally guarded by `expected_etag` and `min_causality`.
- `merge.simulate` to merge two row objects without writing.
- `merge.evaluate` to measure conflict rate, resolution success, and per-case merge timing for example workloads.
- `merge.wasm.register`, `merge.wasm.list`, and `merge.wasm.drop` to manage values-only Wasm merge modules.

Merge policies are stored in `merge_policies.json` format v1. Wasm merge module metadata is stored in `merge_wasm_registry.json` format v1. No on-disk row/value format changes are introduced by v0.3.14.

## 1) Policy Format

A policy has a default merge function and optional per-column overrides:

```json
{
  "default": {"kind":"builtin","name":"last_write_wins"},
  "per_column": {
    "count": {"kind":"builtin","name":"sum"},
    "tags": {"kind":"builtin","name":"set_union"},
    "score": {"kind":"wasm","module_id":"merge_score_sum"}
  }
}
```

Supported function kinds:

- `builtin`: named built-in merge function.
- `wasm`: registered Wasm module id. The registry entry must declare `values_only: true`.

## 2) Built-In Merge Functions

- `last_write_wins` / `replace`: use the incoming value.
- `max`: numeric max.
- `min`: numeric min.
- `sum`: numeric sum. Mixed numeric types promote to `f64`.
- `concat`: string concat or JSON array concat.
- `set_union`: JSON array union by JSON value equality.
- `object_merge`: JSON object merge where incoming keys overwrite current keys.
- `reject`: keep the current value.

If a built-in cannot operate on the provided value types, SkeinDB falls back to the incoming value.

## 3) Wasm Merge Functions

Wasm merge modules execute through the same sandbox used by `skeindb-core` scalar Wasm UDFs. R07 uses a values-only subset:

- ABI: `skein.wasm.udf.v1`.
- Required exports: `memory`, `skein_alloc(len: u32) -> u32`, and the scalar entrypoint, normally `skein_scalar(ptr: u32, len: u32) -> u64`.
- Input: two encoded SkeinQL typed literals, `[current, incoming]`.
- Output: one encoded typed literal returned as `ptr << 32 | len`.
- Capability model: no table access, filesystem, network, clock, randomness, or side-effect hostcalls.
- Limits: `max_fuel`, `max_memory_bytes`, `max_output_bytes`, plus the host wall-clock deadline from the core Wasm runtime.

A Wasm policy is rejected unless the module exists and declares `capabilities.values_only = true`. Infinite loops and other over-budget execution fail the merge operation with an explicit sandbox error rather than corrupting engine state.

Example registration payload:

```json
{
  "method": "merge.wasm.register",
  "params": {
    "module_id": "merge_counter_sum",
    "name": "Counter sum",
    "wasm_b64": "<base64 wasm module>",
    "capabilities": {
      "values_only": true,
      "deterministic": true,
      "max_fuel": 20000,
      "max_memory_bytes": 65536,
      "max_output_bytes": 64
    }
  }
}
```

## 4) Conflict Detection

`merge.apply` accepts `expected_etag`:

- If it matches the current row ETag, the merge is applied normally.
- If it does not match, `conflict=true` is returned and `conflicts=["write_write"]`.
- The merge still applies unless the selected policy rejects or a non-write-write conflict blocks the write.

`merge.apply` also accepts `min_causality` (`etag_chain_v1` or the runtime's causal vector-clock format):

- If dependencies are satisfied, merge execution continues.
- If dependencies are not satisfied, `conflict=true`, `applied=false`, and `conflicts=["dependency"]`.

Constraint conflicts:

- If the incoming row provides primary key values that do not match `pk`, the merge is rejected with `conflicts=["constraint"]`.
- If the merged row violates non-nullable columns, the merge is rejected with `conflicts=["constraint"]`.

## 5) Evaluation Harness

`merge.evaluate` runs a policy against example cases without writing data. It is intended for repeatable research checks, admin-console experiments, and CI-sized safety/performance probes.

Params:

```json
{
  "policy": {
    "default": {"kind":"builtin","name":"last_write_wins"},
    "per_column": {"count":{"kind":"builtin","name":"sum"}}
  },
  "iterations": 10,
  "cases": [
    {
      "name": "counter conflict",
      "current": {"count":{"t":"u64","v":7}},
      "incoming": {"count":{"t":"u64","v":4}},
      "expected_etag_match": false,
      "min_causality_satisfied": true,
      "constraint_ok": true
    }
  ]
}
```

Result shape:

```json
{
  "format": "skein.merge.evaluate.v1",
  "cases": 1,
  "iterations": 10,
  "conflict_count": 1,
  "resolved_count": 1,
  "conflict_rate": 1.0,
  "resolution_success_rate": 1.0,
  "mean_merge_ns": 1200,
  "p95_merge_ns": 1500,
  "results": [
    {
      "name": "counter conflict",
      "conflict": true,
      "conflicts": ["write_write"],
      "resolved": true,
      "merged": {"count":{"t":"u64","v":11}},
      "mean_merge_ns": 1200
    }
  ]
}
```

`iterations` is clamped to a bounded runtime range. Empty case lists are rejected.

## 6) Offline Write Queues

For offline-first clients, use the queue format in `docs/OFFLINE_WRITE_QUEUE.md` to batch `merge.apply` operations and replay them on reconnect. Queue-level `min_causality` is inherited by items unless overridden, and each item can include `expected_etag`, a client-local dependency list, and an optional policy override.

## 7) Current Limits

- Wasm merge functions are values-only scalar functions; they cannot read other rows or tables.
- Multi-row conflict resolution is not implemented.
- Primary key values remain immutable.
- Merge policies are experimental and should be capability-detected through `system.capabilities`.
- Evaluation timing is intended for relative comparisons inside one build/runtime, not cross-machine benchmark claims.
