# Query Patches (query.patch)

Last updated: 2026-01-31

SkeinDB includes a *query-scoped* patch primitive that lets clients update a previously-seen result set with a compact delta rather than re-downloading the full response.

This feature is designed to work alongside web validators (ETags / If-None-Match) and is especially useful when many clients poll the same query.

## What query.patch does

Given:

- a **query** (SkeinQL `Query`),
- optional **args**,
- and a **base_etag** that the client previously received for that *same* query+args,

`query.patch` returns a JSON object with:

- `reset=false` and a **delta** (`added`, `updated`, `removed`, plus optional reorder metadata), or
- `reset=true` (meaning “I can’t produce a safe patch; fetch the full result”), optionally including a `full` result when `include_full=true`.

## Patch caching and coalescing

To reduce fan-out CPU cost, the server maintains an in-memory patch cache keyed by:

- `(base_etag -> current_etag)`

If many clients request the same patch, SkeinDB can reuse a previously computed delta.

The HTTP server also coalesces concurrent `query.patch` calls for the same base_etag (strict JSON mode) so 400 clients arriving “at once” do not all compute the same patch.

## Patchability constraints

A query is patchable if SkeinDB can identify each row uniquely. In this prototype, that means:

- **single-table SELECT**, and
- the table has a **primary key**, and
- the query selects the full primary key columns.

If a query is not patchable, `query.patch` will return an error.

## Window-aware patches (LIMIT/OFFSET)

If the query has a `limit` clause, the patch applies to the *windowed result* (the rows after ORDER BY + LIMIT/OFFSET).

Because inserts/updates can shift membership at the window boundaries, the patch format includes ordering metadata:

- `added[i].at` / `updated[i].at`: the row’s index in the *current window*
- `moved`: explicit per-row moves (within the window)
- `reorder`: a compression hint for large “shift” reorders (e.g., many rows move by +1 due to an insert at the top)

Clients that display ordered lists should apply:

1) `removed`,
2) `updated` (replace by pk),
3) `added` (insert at `at`),
4) `moved` (move by pk),
5) if `reorder.kind == "shift"`, apply the shift to remaining rows except `moved` outliers.

If a reorder would be too large to transmit efficiently (and cannot be compressed), SkeinDB may respond with `reset=true` and `reset_reason="reorder_too_large"`.

## Response format

The patch result is returned as a JSON object (inside the normal JSON-RPC envelope) with the following fields.

### Common fields

- `reset: bool` — true if the client must refresh fully.
- `base_etag: string | null` — echoed from request.
- `base_source: string` — where the server got the “old” state:
  - `etag_cache` — server has the base snapshot cached
  - `patch_cache` — server had a cached `(base->current)` delta
  - `client_state` — client provided `client_state.rows`
  - `client_bloom_add_only` — client provided a bloom filter and requested add-only mode
- `reset_reason: string` — present when `reset=true`.
- `partial: bool` — present for best-effort modes.
- `removed_unknown: bool` — the server did not (or could not) compute removals.
- `updated_unknown: bool` — the server did not (or could not) compute updates.
- `columns: ColumnMeta[]` — column metadata for the current result.
- `window: { limit?: u64, offset?: u64 }` — present when the query includes LIMIT/OFFSET.

### Delta fields

- `added: PatchRow[]`
- `updated: PatchRow[]`
- `removed: { pk: Lit[] }[]`
- `moved?: { pk: Lit[], from: number, to: number }[]`
- `reorder?: { kind: "shift", delta: number, coverage: number }`
- `added_batch?: WasmBatch`
- `updated_batch?: WasmBatch`

`PatchRow` is format-dependent:

- For `result_format=rows_json`:
  - `{ pk: Lit[], at: number, row: Lit[] }`
- For `result_format=objects_json`:
  - `{ pk: Lit[], at: number, obj: Record<string, Lit> }`
- For `result_format=skeinpack_v1` with `wire.format="skeinpack_v1"`:
  - `{ pk: Lit[], at: number, row: SkeinpackRow }`
  - plus a top-level `dict` in the patch payload.
- For `result_format=wasm_batch_v1`:
  - `{ pk: Lit[], at: number, row_idx: number }`
  - plus `added_batch` / `updated_batch` (each a `WasmBatch`).

`WasmBatch` is the same shape returned by `query.select` with `result_format=wasm_batch_v1`:

```json
{
  "format": "skein.wasm.batch.v1",
  "columns": [ {"name":"id","type":{"kind":"u64"}} ],
  "batch_b64": "..."
}
```

### Full result on reset

If `reset=true` and `include_full=true`, the patch will include:

- `full: <same shape as query.select>`

This is a convenience to avoid a second round-trip.

## Client-state fallback (avoid resets after cache eviction)

If the server does not have the base snapshot available (cache eviction, restart, etc.), the client can supply its own compact view of the base window.

Request field:

- `client_state.rows: [{ pk: Lit[], fp?: string }]`

Where `fp` is the row fingerprint previously computed by the client.

Modes:

- `client_state.mode = "strict"` (default): all rows must include `fp` or the server will reset.
- `client_state.mode = "membership_only"`: server computes added/removed but sets `updated_unknown=true`.
- `client_state.mode = "add_only"`: server may use a bloom filter to compute *additions only*.

### Bloom filter add-only mode

For very large windows, sending every pk/fp may still be expensive. In that case, the client may provide:

- `client_state.mode = "add_only"`
- `client_state.keys_bloom = { bits_b64, m_bits, k, salt? }`

In this mode, the patch is best-effort:

- `added` rows are those *definitely not* in the bloom filter
- `removed_unknown=true` and `updated_unknown=true`

This can be useful for feeds or dashboards where occasional stale items are acceptable and periodic full refreshes are still performed.

## Notes on ETags

SkeinDB query ETags incorporate:

- the canonical query JSON,
- the query args,
- and dependency versions (tables referenced by the query).

So the same query text with different args produces different ETags.
