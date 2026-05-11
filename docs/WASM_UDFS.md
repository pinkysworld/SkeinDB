# Wasm UDFs with Capabilities and Safe Cancellation

Status: Draft + hardened merge-function integration
Last updated: 2026-05-11

Goal:
Allow extensions (scalar UDFs, aggregates, table functions) in a sandboxed runtime with strict resource limits, explicit capabilities, and safe cancellation.

This feature is designed to be optional and off by default in strict compatibility deployments.

---

## 1) Threat model

Assume extension code may be buggy or malicious.
The system MUST protect:
- availability (no infinite loops or memory bombs)
- confidentiality (no unauthorized reads)
- integrity (no unauthorized writes)

---

## 2) UDF types

1) Scalar UDF
- input: one row's arguments
- output: a single value

2) Aggregate UDF
- input: per-row args
- state: accumulator
- output: final value

3) Table function
- input: args
- output: a stream of rows

---

## 3) Capability model

Each installed module has a manifest:

- allowed_hostcalls: list
- allowed_tables: list of (db, table) with read/write flags
- deterministic: bool
- max_fuel: u64 (instruction budget)
- max_memory_bytes: u64
- max_output_bytes: u64

Default policy:
- no filesystem
- no network
- no clock
- no randomness

The host only exposes a function if the manifest allows it.

---

## 4) ABI (v1 recommendation)

Keep ABI minimal and explicit.

### 4.1 Value encoding

Arguments and results use a compact tagged encoding (similar to SkeinQL typed literals), serialized into a byte buffer.

Row sets use a similarly compact nested encoding:

- `row_count: varu`
- per row: `value_count: varu`
- per value: tagged value bytes

### 4.2 Function signatures

Exported functions (examples):
- skein_scalar(ptr: u32, len: u32) -> u64
- skein_aggregate(ptr: u32, len: u32) -> u64
- skein_table(ptr: u32, len: u32) -> u64

Where:
- the host writes args into module memory at (ptr,len)
- aggregate modules receive a row-set buffer and return one encoded value
- table modules receive an args buffer and return an encoded row-set buffer
- the module writes result rows/values into module memory and returns a packed (ptr,len) in u64

This avoids exposing host pointers and keeps memory ownership clear.

---

## 5) Safe cancellation

Cancellation must be reliable.

Recommended approach:
- instruction metering (fuel) with a maximum per call
- optional wall-clock timeout at the host layer

If a module exceeds its budget:
- trap the instance
- abort the query (or treat as UDF error based on policy)

Important:
- cancellation must not corrupt engine state
- UDFs should be side-effect free by default

---

## 6) Determinism

For query caching, replication, and consistent results, deterministic UDFs are valuable.

Policy:
- deterministic=true modules cannot call clock/random/network
- deterministic=false modules are allowed but cannot be used in cached queries (ETag) unless explicitly configured

---

## 7) Installation and management

SkeinQL methods:
- udf.install
- udf.list
- udf.drop

SQL compatibility (optional):
- CREATE FUNCTION ... LANGUAGE wasm ...

Store modules as immutable blobs in the ValueStore.
Reference them from catalog metadata.

Current `skeindb-core` implementation status for T080:

- Wasm module bytes can now be stored immutably in `ValueStore` as `ValueKind::BlobChunk` entries.
- `WasmModuleCatalog` persists UDF metadata to `wasm_catalog.json` (format v1), separate from the older merge-specific `merge_wasm_registry.json` prototype.
- Catalog entries track module id, optional name, UDF kind, ABI, entrypoint symbol, `ValueId`, byte size, creation time, and capability metadata.
- The catalog supports install/list/get/drop plus byte materialization back through `ValueStore`.

Current `skeindb-core` implementation status for T081:

- Scalar Wasm UDF execution is now available in `crates/skeindb-core/src/wasm_udf.rs` via `execute_scalar_udf(...)`.
- The current core execution ABI is `skein.wasm.udf.v1` with:
  - exported `memory`
  - exported allocator `skein_alloc(len: u32) -> u32`
  - scalar entrypoint export (usually `skein_scalar(ptr: u32, len: u32) -> u64`) returning `ptr<<32 | len`
- Resource limits enforced today:
  - memory cap from `max_memory_bytes` (defaulting to a conservative sandbox limit when omitted)
  - output size cap from `max_output_bytes` (also defaulting to a conservative limit when omitted)
- Capability-gated hostcalls are supported for the current `skein.log_debug` hostcall mapped from `allowed_hostcalls = ["log.debug"]`.
- Filesystem, network, clock, and randomness remain unavailable because no such imports are defined.

Current `skeindb-core` implementation status for T082:

- Scalar Wasm execution now applies manifest fuel budgets when `max_fuel > 0`, using Wasmtime fuel metering to terminate deterministic infinite loops with an explicit `FuelExhausted` UDF error.
- The host also wraps each scalar call in a bounded wall-clock deadline using epoch interruption. The default timeout is conservative (`1s`) and embedders/tests can override it through `execute_scalar_udf_with_options(...)`.
- `max_fuel = 0` now means "no explicit fuel budget" rather than "run forever": the wall-clock deadline still provides safe cancellation.
- Cancellation errors are surfaced distinctly from generic traps: out-of-fuel maps to `FuelExhausted`, and epoch interruption maps to `TimeoutExceeded`.
- Integration coverage now includes deterministic fuel exhaustion, host timeout cancellation, and a recovery path showing later UDF executions still succeed after a cancelled one.

Current `skeindb-core` implementation status for T083:

- Aggregate and table Wasm execution now share the same sandbox runtime in `crates/skeindb-core/src/wasm_udf.rs` as scalar UDFs, so memory limits, output limits, capability checks, fuel budgets, and wall-clock cancellation all apply uniformly.
- Aggregate modules are executed via `execute_aggregate_udf(...)`, which sends a one-shot encoded row batch to the module entrypoint and expects one encoded scalar result back.
- Table modules are executed via `execute_table_udf(...)`, which sends scalar args to the module entrypoint and expects an encoded row set back.
- Shared `encode_rows(...)` and `decode_rows(...)` helpers now define the row-batch ABI used by aggregate inputs and table outputs.
- Integration coverage now includes an aggregate module that sums row values and a table module that materializes rows from input arguments.

---

## 8) Testing requirements

1) Resource limit tests
- infinite loop must be cancelled
- memory growth beyond limit must fail

2) Isolation tests
- module without capability cannot read tables

3) Correctness tests
- simple scalar UDF produces expected values

4) Fuzzing (recommended)
- fuzz the host<->wasm value encoding boundary

---

## Research extensions: Merge functions and Wasm query operators

Two research directions in the agenda build directly on the Wasm sandbox:

1) **Client-side merge functions** for optimistic concurrency (R07)
2) **Wasm-native query operators** (R19)

See:
- `docs/research_agenda/R07_optimistic-concurrency-with-client-side-merge-functions.md`
- `docs/research_agenda/R19_webassembly-native-query-operators.md`

Integration sketches:
- Merge functions should be executed as sandboxed Wasm code with a "values-only" capability set: they receive conflicting versions and must return the resolved value without arbitrary DB reads.
- Wasm query operators require a stable ABI for batches; start with filter/project on columnar batches and expand gradually.
  See `docs/WASM_OPERATORS.md`.

Current R07 merge-function integration:
- `merge.wasm.register` stores immutable module bytes through the core `ValueStore` / `WasmModuleCatalog` path and persists merge-facing registry metadata in `merge_wasm_registry.json` format v1.
- `merge.apply`, `merge.simulate`, and `merge.evaluate` execute policy entries of the form `{"kind":"wasm","module_id":"..."}` when the registered module declares `capabilities.values_only = true`.
- Merge modules use the scalar `skein.wasm.udf.v1` ABI (`memory`, `skein_alloc`, and normally `skein_scalar`) with two encoded typed-literal arguments: current value and incoming value.
- The result must be one encoded typed literal. Table access, filesystem, network, clock, randomness, and side-effect hostcalls are unavailable in the merge path.
- Core sandbox limits apply to merge execution: fuel exhaustion, memory/output limits, and wall-clock timeout surface as merge errors and do not mutate engine state.
