# Query Coalescing (In-flight Deduplication)

Status: Partial implementation
Last updated: 2026-03-27

Current runtime baseline:
- `GET /api/v1/q/{query_id}` coalesces unconditional prepared-query reads by `query_id`.
- `query.patch` coalesces strict JSON patch requests sharing the same `base_etag -> current_etag` transition.
- Metrics, auth-scoped fingerprints, cancellation semantics, and broader entry-point coverage remain follow-on work.

Goal:
Reduce server load and tail latency under bursty traffic by preventing the "thundering herd" problem:
when many clients request the same expensive query at once, the server should execute it once and
share the result.

This is a performance feature that complements:
- HTTP cache validators (ETags) (docs/ETAG_VALIDATORS.md)
- prepared queries (GET /api/v1/q/{query_id})
- telemetry (docs/TELEMETRY_AND_MIGRATION.md)

---

## 1) Scope and safety

Query coalescing is only safe when the result is deterministic for a given input.

SkeinDB defines a query as **coalescable** iff all of the following hold:

1) It is a read-only query (no DDL/DML).
2) It has no non-deterministic functions (e.g., random(), uuid(), now() unless pinned).
3) It does not depend on mutable session state (e.g., user variables).
4) It executes under a compatible snapshot policy:
   - explicit snapshot timestamp (read_at.ts)
   - OR a stable "group snapshot" policy used only for cacheable HTTP endpoints

MySQL protocol sessions may opt into coalescing explicitly.
SkeinQL cacheable GET endpoints may enable it by default.

---

## 2) Canonical query fingerprint

Coalescing groups requests by a fingerprint:

fingerprint = hash(
  query_id_or_signature,
  canonical_args,
  read_policy,
  tenant_or_auth_scope,
  selected_format
)

Where:
- query_id_or_signature is the prepared query_id (preferred) or a canonical SkeinIR signature.
- canonical_args is stable (sorted keys, normalized numbers).
- tenant/auth scope prevents data leakage across users.
- selected_format ensures the result payload is identical.

Note:
- Coalescing should treat projection differences as different fingerprints.

---

## 3) Coalescing architecture

### 3.1 In-flight map

Maintain a concurrent map:

Map<Fingerprint, InFlightQuery>

InFlightQuery contains:
- started_at
- leader_request_id
- waiters count
- cancellation tokens for waiters
- shared execution handle (task/future)
- shared result buffer (or streaming fan-out handle)

### 3.2 Leader / joiner protocol

On request arrival:

1) Compute fingerprint.
2) If map has an InFlightQuery:
   - register as joiner
   - await leader completion
   - return the same response payload
3) Otherwise:
   - insert new InFlightQuery as leader
   - execute query
   - publish result to joiners
   - remove from map

### 3.3 Coalescing window (optional)

A small window (e.g., 2–10 ms) can increase coalescing hit rate:
- leader delays execution briefly to gather joiners
- use only when latency budget allows

---

## 4) Interaction with ETags

Coalescing is most valuable when the server cannot return 304.

Recommended flow for GET /api/v1/q/{query_id}:

1) If If-None-Match is present:
   - compute ETag cheaply if possible (dependency-set ETag)
   - if match -> return 304 immediately
2) Otherwise, execute query:
   - coalesce in-flight executions by fingerprint
   - include computed ETag in response

This reduces repeated heavy query execution on cache misses and during bursts.

---

## 5) Cancellation and resource limits

### 5.1 Joiner cancellation

If a joiner disconnects/cancels:
- remove it from waiters
- do NOT cancel the leader automatically

### 5.2 Group cancellation

If *all* waiters and the leader cancel:
- cancel the underlying execution if supported by the executor

### 5.3 Limits

Protect the server with limits:
- max_inflight_groups
- max_waiters_per_group
- max_result_bytes_to_fanout (fallback: do not coalesce, or cache to disk)

---

## 6) Metrics

Expose:
- coalesce_groups_created_total
- coalesce_joiners_total
- coalesce_hit_rate
- coalesce_wait_ms_p50/p95
- coalesce_aborted_total
- coalesce_max_waiters_observed

---

## 7) Implementation phases

Phase 1:
- Coalescing for prepared queries executed via GET /api/v1/q/{query_id}
- Coalescing for query.patch (strict JSON mode; many clients share one computed patch)
- In-memory shared result buffer for rows_json only

Phase 2:
- Coalescing for POST query.select (SkeinQL)
- Optional coalescing window

Phase 3:
- Safe opt-in coalescing for MySQL sessions
- Streaming fan-out for large results

---

## 8) SkeinQL surfaces

Recommended settings (admin-only):
- `settings.set` { "query_coalesce": { "enabled": true, "window_ms": 5, "max_groups": 1000 } }

Recommended stats fields (stats.snapshot):
- `coalescing.enabled`
- `coalescing.groups_inflight`
- `coalescing.hit_rate`

---

## 9) Backlog

- QC01: query fingerprint + canonicalization utilities
- QC02: in-flight query map + leader/joiner
- QC03: GET /api/v1/q coalescing + tests
- QC04: metrics + dashboards
