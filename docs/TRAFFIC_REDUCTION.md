# Traffic Reduction Features (Design Notes)

This document captures SkeinDB/SkeinQL design options intended to **reduce network traffic** and **client parsing load** under high fan-out read workloads.

The design is general (healthcare was only an example workload): the same mechanisms apply to dashboards, SaaS admin panels, IoT control planes, multi-tenant portals, chat-like apps, and any environment with many concurrent readers.

SkeinDB's stance:
- Prefer **correctness-first** primitives (ETag validators + dependency tracking) so caches can be aggressive without returning stale/incorrect data.
- Make traffic-saving encodings **opt-in** and **capability-negotiated**.
- Ensure every traffic reduction mechanism is compatible with authorization and multi-tenant isolation.

---

## 1) Conditional reads as a first-class database primitive

### 1.1 Query ETags
SkeinDB can compute an ETag for a query result based on its dependency set.
Clients revalidate using `if_none_match` so unchanged screens cost ~0 bytes:

- Client: `query.select(cache.if_none_match = "W/\"...\"")`
- Server: `{ not_modified: true }` (or HTTP 304 if using cacheable GET endpoints)

This reduces:
- bandwidth
- client JSON parsing cost
- server CPU (when ETag short-circuit is possible)

### 1.2 Query result caching with exact invalidation
Because dependency sets identify what data a query depends on, the server can maintain a result cache that is **exactly invalidated** when those dependencies change.

This complements "query coalescing" (thundering herd protection) by also accelerating repeated reads in steady state.

---

## 2) Push/stream instead of polling

### 2.1 Query-scoped CDC (dependency-driven)
Traditional CDC streams tables; many modern applications want "notify me when *this query* changes".

SkeinDB can expose:
- invalidation-only: "the query changed, refetch"
- delta patches: "rows added/removed/updated" for the query

Because dependency tracking already exists for ETags, it can be reused for query-scoped subscriptions.

### 2.2 QueryPatch (query-scoped deltas)
When a query changes *slightly*, ETag revalidation still forces a full refetch.

QueryPatch adds a dedicated method (`query.patch`) that returns minimal deltas:
- rows **added**
- rows **updated**
- rows **removed**

This is especially useful for list queries under high fan-out read traffic.

See: `docs/QUERY_PATCH.md`

#### 2.2.1 Server-side patch cache + in-flight coalescing
To handle bursts (hundreds of clients refreshing at once), the server can:
- **coalesce** in-flight patch requests so a single computation is shared
- **cache** computed deltas keyed by `base_etag -> current_etag` so later clients reuse the same patch

This keeps CPU cost closer to O(1) per burst instead of O(N clients).

#### 2.2.2 Window-aware patches and reorder hints
For paginated or bounded list queries (LIMIT/OFFSET), SkeinDB treats the response as a *window*.
Patches may include:
- `window`: the LIMIT/OFFSET of the patched view
- `moved`: per-row move events (from/to indices)
- `reorder`: a compressed hint (e.g., a uniform shift) that avoids listing every move when a new item appears at the top

This makes patches useful for UI lists that require stable ordering.

#### 2.2.3 Client-known summaries for reset avoidance
If the server no longer has the cached base snapshot (eviction / restart), it can still avoid a full reset if the client provides a compact summary of what it currently has:
- `client_state.rows`: primary keys + per-row fingerprints (`fp`)

For extremely large windows, a best-effort fallback is supported:
- `client_state.mode="add_only"` with `client_state.keys_bloom`

In add-only mode the server returns only rows that are definitely new, and marks removals/updates as unknown.

---

## 3) Shape projection and field masks

When a UI needs only a subset of columns, do not transfer the full row.

SkeinQL supports projection in the query AST. Deployments may also define named "shapes":
- `PatientSummary`, `UserListItem`, `OrderHeader`, etc.

Named shapes reduce payload size and increase cache hit rates because summaries often change less frequently than full objects.

---

## 4) Optional dictionary transfer: `skeinpack_v1`

### 4.1 Motivation
JSON payloads frequently repeat values:
- status strings, enum values
- tenant names, regions, categories
- repeated keys and labels
- repeating small objects

SkeinDB is ValueID-first internally. `skeinpack_v1` exposes an optional wire encoding that uses that fact.

### 4.2 Idea
- Server sends rows where large/repeated values are referenced by stable `ValueID`s.
- Alongside rows, server sends a dictionary of `ValueID -> literal` for values the client is missing.
- Client caches the dictionary locally.
- Future requests send a compact summary of the cached dictionary (Bloom filter or explicit list) so the server sends only missing entries.

This is similar in spirit to "dictionary compression", but at the protocol boundary.

### 4.3 Safety requirements
- Dictionary entries MUST be scoped to the client's authorization context (avoid cross-user leaks).
- Servers MUST be able to disable dictionary transfer globally.
- Clients MUST handle fallback to standard JSON rows.

### 4.4 Research evaluation hooks
A clean experimental evaluation includes:
- bytes transferred per UI refresh vs. baseline JSON + gzip
- CPU overhead on server for building dict deltas
- CPU overhead on client for dict lookups
- tail latency under 100s–1000s concurrent clients

---

## 5) Transport-level best practices

Even with protocol-level optimizations, deployments should use:
- HTTP compression (gzip/brotli)
- HTTP/2 multiplexing
- optional binary encodings (MessagePack/CBOR) for mobile SDKs

A QUIC/HTTP/3 transport is listed in the research agenda (R09).
