# Cache-coherent Queries with HTTP Validators (ETags)

Status: Partial implementation
Last updated: 2026-03-27

Current runtime baseline:
- `data.get` returns row ETags and `data.update` honors `expected_etag` / `If-Match`-style optimistic writes.
- `query.select` and `query.execute_prepared` can return query ETags and honor `if_none_match`.
- `vector.search` returns source-table dependency metadata, V2 causality tokens, and vector-search ETags by default; it honors `cache.if_none_match` with a `not_modified=true` result and invalidates when `data.insert`, `data.update`, `data.delete`, or `vector.insert` changes the source table version.
- `query.prepare` plus `GET /api/v1/q/{query_id}` is live and returns HTTP ETag headers with `304 Not Modified` on matches.
- `query.patch` reuses the same query-level ETag surface for delta refreshes.
- `query.subscribe` exposes an SSE endpoint for prepared-query invalidation, and `cdc.subscribe_query` exposes the same dependency-driven invalidation model over both polling and `GET /api/v1/cdc/sse/{sub_id}` with query ETags.

Goal:
Make SkeinDB a web-native database by supporting cache-coherent reads using HTTP validators.

If a client supplies If-None-Match: <etag> and the data has not changed, SkeinDB should return HTTP 304 Not Modified.

This feature is designed to be sound (never return 304 when the response would differ).

---

## 1) Scope

We define two levels of caching:

1) Row ETags (easy, high value)
- Cache individual rows fetched by primary key.

2) Query ETags (harder, high leverage)
- Cache list and search queries.

---

## 2) Row ETags

For a primary key fetch, SkeinDB can generate a strong ETag based on MVCC version identity.

Recommended construction:
- etag = "row:" + base64url(hash(table_id || pk || head_begin_ts || head_end_ts || head_valueids_hash))

Notes:
- Using commit timestamps and/or ValueIDs ensures ETag changes when any column changes.
- The ETag is computed for the row as returned under the request snapshot.

Conditional semantics:
- GET row with If-None-Match:
  - return 304 if etag matches
  - else return 200 with row and etag

- UPDATE row with If-Match:
  - apply update only if current row etag matches
  - else return 409 conflict

This enables optimistic concurrency for web clients.
The `merge.apply` method follows the same pattern and reports conflicts while
applying merge functions when configured.

---

## 3) Query ETags

Correct query ETags require tracking which parts of storage could affect the result.

### 3.1 Dependency sets

The planner should output a dependency set alongside the physical plan.

A dependency set is a list of "version sources".
Example:
- { table_id, access_path, key_range, sources: [run_id, run_epoch] }

### 3.2 ETag from LSM components (recommended design)

SkeinDB uses LSM-like sorted runs.
Each immutable run has:
- run_id
- min_key, max_key
- run_epoch (monotonic)

For a range scan over an index:
- include every run whose [min_key,max_key] overlaps the scan range
- include the current memtable epoch for that index

ETag = hash(
  query_signature,
  # query_signature should include the normalized query + args

  for each dependency source: (run_id, run_epoch),
  mem_epoch
)

Properties:
- Writes outside the queried range create runs that do not overlap; they do not affect the ETag.
- Writes inside the range create new memtable entries and later new runs that overlap; ETag changes.

This is sound if overlap detection and mem_epoch inclusion are sound.

---

## 4) Persisted queries (to enable HTTP GET caching)

HTTP caches generally cache GET responses (not POST).

Recommendation:
- Use query.prepare to register a canonical query and obtain query_id.
- Execute via GET /api/v1/q/{query_id}?args=...

The server should:
- compute and return ETag header
- honor If-None-Match

This also makes URLs stable and shareable.

---

## 5) Subscriptions

ETags enable lightweight change notification.

query.subscribe can:
- return a stream of "etag changed" events when dependencies change

Transport options:
- SSE (server-sent events)
- WebSocket

---

## 6) Implementation phases

Phase 1:
- Row ETags for data.get
- If-Match on data.update

Phase 2:
- Query ETags for simple queries (single table, indexed predicates)
- query.prepare + GET execution

Phase 3:
- Join query dependency sets
- More precise memtable epochs (bucketed)

---

## 7) Metrics

Expose:
- etag_hit_rate
- 304_rate
- avg_etag_compute_time
- query_dependency_size

---

## Research extension: Causal consistency via ETag chains

The baseline design uses ETags as **freshness validators**.
The research agenda proposes extending ETags to encode **causal dependencies** so clients can obtain session/casual consistency using standard HTTP mechanics.
See: `docs/research_agenda/R13_causal-consistency-via-etag-chains.md`.

Sketch:
- Each response includes a “causality token” alongside ETags.
- Clients propagate `min_causality` on subsequent requests.
- The server ensures returned results are consistent with all operations causally visible to that token.

Current runtime format (`vector_clock_v2`):
- Token format:
  - `{"format":"vector_clock_v2","deps":[{"table":"app.users","v":3}]}`
- `deps` is a dependency list similar to `deps.tables` in query results.
- `query.select`, `query.execute_prepared`, and `vector.search` emit `vector_clock_v2` causality tokens in responses.
- Requests may still send legacy `etag_chain_v1` tokens while older clients are migrated; `ensure_min_causality()` accepts both formats.
- Replicated write fanout reuses the same token format through `x-skeindb-replication-causality`, and replicas merge the applied watermark into `cluster.status.replication.causality` and `stats.snapshot.cluster.replication.causality`.
- A request with `cache.min_causality` is rejected if any overlapping dependency has a lower version.
- `If-None-Match` can be combined with `min_causality`: if the ETag matches and the dependency floor is satisfied, the server returns `not_modified:true`; if the causality floor is ahead of current dependencies, the request fails with `precondition_failed`.
- Servers may advertise support via `system.capabilities.causal_etags`.

Implementation notes:
- Full vector clocks may be too large; consider compressed dependency sets or hybrid schemes.
- Interactions with intermediate caches must be explicitly specified (e.g., caches may treat causal validators as uncacheable unless policy says otherwise).
