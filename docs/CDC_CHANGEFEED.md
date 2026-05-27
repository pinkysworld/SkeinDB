# Change Data Capture (CDC) and dependency-driven changefeeds

Status: Partial implementation
Last updated: 2026-05-27

Current runtime baseline:
- `cdc.subscribe_table` creates table subscriptions over the RPC API. Subscriptions can request row images with `include: {"before": true, "after": true}`, can narrow delivery to selected source mutation kinds with `ops: ["insert", "update", "delete"]`, can restrict replay to one exact primary-key tuple with `pk: [<typed Lit>, ...]`, can restrict replay to an inclusive primary-key range on single-column primary keys with `pk_range: {"lower_bound": <typed Lit>?, "upper_bound": <typed Lit>?}`, can restrict replay to mutations where at least one named column value changes with `columns: ["status", ...]`, and can choose `format: "objects_json"` or `format: "plain_json"` for `cdc.poll`, SSE, and WebSocket event payloads.
- `cdc.subscribe_query` creates dependency-driven subscriptions for prepared queries and emits invalidation events with the current query ETag. Query subscriptions can request the same optional triggering row images through `include`, and the same `ops`, exact `pk`, inclusive `pk_range`, and changed-column `columns` filters constrain which source table mutations emit `invalidate` events. Prepared queries over views expand those view dependencies to the underlying base tables that actually emit retained CDC events, prepared queries with set-operation bodies (for example `UNION` / `UNION ALL`) track dependency tables across every branch, and prepared queries using CTEs walk the CTE definitions for base-table dependencies while ignoring the CTE names as physical tables.
- `cdc.poll` reads from the retained persisted change log and returns `earliest_offset` / `latest_offset`, `resnapshot_required` metadata when a consumer falls behind the retained horizon, and a `backpressure` object with `state`, `lag`, `remaining_until_resnapshot`, and `paused`.
- `cdc.pause` and `cdc.resume` persist an operator pause flag per subscription. Paused subscriptions keep their cursor and retained-horizon accounting, but `cdc.poll` suppresses data events until resumed unless a resnapshot is already required.
- `GET /api/v1/cdc/sse/{sub_id}` streams the same subscription events over SSE, with replay from the retained change log, bounded batch delivery, reconnect via `Last-Event-ID` or `from_offset`, `backpressure` control events when a subscription enters a non-healthy state, and a terminal `resnapshot` control event when the reconnect cursor falls behind retention.
- `GET /api/v1/cdc/ws/{sub_id}` upgrades to a WebSocket stream that replays the same retained subscription events as JSON text frames, resumes from `Last-Event-ID` or `from_offset`, emits `backpressure` control frames for non-healthy states, and emits a terminal `resnapshot` control frame when the reconnect cursor falls behind retention.
- `cdc.ack` persists the acknowledged consumer cursor per subscription so polling, SSE, and WebSocket resumes survive process restarts.
- `cdc.close` removes the subscription handle and its persisted cursor state.
- `stats.snapshot.cdc` reports active subscription counts, aggregate lag, retained-window offsets, pause/pressure counters, backpressure thresholds, and `dropped_events_total` so operators can spot consumers that have fallen behind retention.
- SkeinAdmin now includes a dedicated CDC page for `cdc.subscribe_table` / `cdc.poll` / `cdc.pause` / `cdc.resume` / `cdc.ack` / `cdc.close`, with lag and backpressure visualization derived from the current poll/control state.
- Subscribe results now include both `sse_url` and `ws_url` transport paths.

SkeinDB's MySQL compatibility mode is valuable for adoption, but modern web and data pipelines often need a "push" interface for changes.
This document specifies a CDC subsystem that provides:

1. table-level change streams (insert/update/delete)
2. query-level changefeeds driven by prepared-query dependency sets (pairs naturally with ETags)

The novelty for SkeinDB is that query-level CDC can be derived from the same dependency metadata used for cache coherency.

## 1. Goals

- Provide low-latency incremental change propagation.
- Support both pull and push consumption patterns.
- Be safe for a single-binary deployment (no external Kafka requirement).
- Integrate with clustering later.

Non-goals (v1):
- Full MySQL replication protocol compatibility.

## 2. Event model

Each CDC event includes:
- `offset`: monotonically increasing sequence (per stream)
- `commit_ts_ms` and (optional) `lsn`
- `db`, `table`
- `op`: insert | update | delete
- `pk`: primary key values
- `before` and `after` images (optional, configurable)

Current runtime scope:
- row-level table events persist `commit_ts_ms`, `lsn = seq`, and optional `before` / `after` row images in the retained change log; `cdc.poll`, SSE, and WebSocket only expose those images for subscriptions that opt in via `include.before` / `include.after`, only deliver source events whose op matches the optional subscription `ops` allowlist, whose retained `pk` exactly matches the optional subscription `pk` tuple and/or falls within the optional inclusive `pk_range` bounds, and when `columns` is present only deliver events whose retained `before` / `after` row images differ on at least one selected column while projecting exposed row images down to those columns
- query invalidation events reuse the triggering table event metadata and override `op = "invalidate"`; when requested, they can also forward the triggering event's `before` / `after` row images alongside `query_id` / `etag`, and the optional subscription `ops` allowlist, exact `pk` tuple, inclusive `pk_range`, and changed-column `columns` filter are evaluated against the underlying triggering table event before the invalidation is emitted
- delivery formats are subscription-scoped and persisted: `objects_json` keeps `pk`, `before`, and `after` values as typed SkeinQL `Lit` envelopes, while `plain_json` emits those same fields as ordinary JSON scalars/objects for clients that do not need type tags

## 3. Streams

### 3.1 Table streams

- `cdc.subscribe_table` creates a stream for a given table.
- Current runtime scope: full-table subscriptions plus optional exact and inclusive primary-key filtering.
- Current runtime scope: `include.before` / `include.after` opt into full-row images for insert/update/delete events.
- Current runtime scope: `ops` can narrow a full-table stream to `insert`, `update`, and/or `delete` source events.
- Current runtime scope: `pk` can narrow a full-table stream to one exact primary-key tuple; table subscriptions reject `pk` filters when the table has no primary key or the tuple width does not match.
- Current runtime scope: `pk_range` can narrow a full-table stream to retained events whose single-column primary key falls inside inclusive `lower_bound` / `upper_bound` bounds; table subscriptions reject range filters when the table has no primary key, has a composite primary key, when both bounds are missing, or when the lower bound sorts after the upper bound.
- Current runtime scope: `columns` can narrow a stream to events where at least one selected column value changes between the retained `before` / `after` row images; when row images are included, they are projected down to those columns, and table subscriptions reject unknown column names.
- Current runtime scope: `format` defaults to `objects_json`; `plain_json` is also accepted and applies consistently to `cdc.poll`, SSE, and WebSocket delivery.
- Optional filters beyond the current exact/range primary-key, source-op, and changed-column filters (shard) remain planned.

### 3.2 Query streams (dependency-driven)

- `cdc.subscribe_query` subscribes to changes that might affect a prepared query.
- The server computes a dependency set for the query (tables, key ranges, or index ranges).
- Whenever a commit intersects the dependency set, the server emits a "query invalidated" event:
  - query_id
  - new_etag
  - changed_keys summary (optional)

Current runtime scope:
- dependency sets are conservative and table-based, reusing the same prepared-query dependency metadata used for query ETags; when a prepared query references one or more views, subscription delivery expands those dependencies to the underlying base tables that actually emit retained CDC events, when a prepared query uses a set operation the dependency set includes tables referenced by every branch, and when a prepared query uses CTEs the runtime walks each CTE definition for base tables without treating the CTE relation name as a retained-event table
- invalidation events are delivered through `cdc.poll` with `op = "invalidate"`, the triggering `db` / `table`, optional `query_id` / `etag` fields, optional triggering `before` / `after` row images when requested, and optional source-op, exact-primary-key, inclusive-primary-key-range, plus changed-column filtering through the same `ops`, `pk`, `pk_range`, and `columns` options used by table subscriptions
- subscriptions are bound to a prepared `query_id` plus its positional args, and the runtime recomputes the effective base-table change set from the stored query during polling so older durable query subscriptions keep invalidating correctly after restart

This gives applications a direct way to update caches and UIs.

## 4. Delivery mechanisms

Current runtime support:
- Long poll: `cdc.poll` (RPC)
- SSE: `GET /api/v1/cdc/sse/{sub_id}`
  - emits the same JSON event payloads as `cdc.poll`
  - replays from `?from_offset=<seq>` or `Last-Event-ID: <seq>`
  - drains the retained change log in bounded batches so slow consumers can reconnect without losing events inside the retained horizon
  - emits `event: backpressure` when the subscription enters `pressure`, `throttle`, `paused`, or `resnapshot_required`
  - emits `event: resnapshot` with recovery metadata when the reconnect cursor falls behind retention
- WebSocket: `GET /api/v1/cdc/ws/{sub_id}`
  - upgrades over HTTP and streams JSON text frames shaped as `{"id": <seq>, "event": "insert|update|delete|invalidate|backpressure|resnapshot", "data": {...}}`
  - replays from `?from_offset=<seq>` or `Last-Event-ID: <seq>`
  - drains the retained change log in bounded batches so reconnecting consumers can catch up inside the retained horizon
  - emits `backpressure` control frames when the subscription enters `pressure`, `throttle`, `paused`, or `resnapshot_required`
  - emits a final `resnapshot` control frame with the same recovery metadata returned by `cdc.poll`

Backpressure state model:
- `healthy`: lag is below the warning threshold.
- `pressure`: lag is at or above `warn_lag` (half the retained window, minimum 1 event).
- `throttle`: lag is at or above `throttle_lag` (three quarters of the retained window, rounded up).
- `paused`: an operator paused the subscription with `cdc.pause`; retained-horizon checks still win if the cursor is already too old.
- `resnapshot_required`: the acknowledged cursor fell behind `earliest_offset`, so the consumer must rebuild from a snapshot before resuming.

## 5. Exactly-once vs at-least-once

- Default delivery is at-least-once.
- Consumers ACK offsets via `cdc.ack`.
- Current runtime persists acked offsets per subscription in `cdc_subscriptions.json` and suppresses redelivery for older offsets after restart as well as within a live process.
- SSE reconnects are driven by event `id = seq`; clients can resume by supplying `Last-Event-ID` or `from_offset`.
- WebSocket reconnects use the same `id = seq` cursor and accept `Last-Event-ID` or `from_offset` on the upgrade request.
- When a consumer falls behind the retained horizon, `cdc.poll` returns `resnapshot_required = true` and SSE/WebSocket emit a `resnapshot` control event instead of replaying a partial stream.
- `cdc.pause` persists `paused = true` in `cdc_subscriptions.json`; `cdc.resume` clears it. Pausing does not ACK events or extend retention, so a paused consumer can still cross into `resnapshot_required`.

## 6. Retention

Planned target design:
- retention is tied to WAL retention
- if WAL is truncated before a consumer catches up, the subscription must resnapshot

Current runtime note:
- the persisted retained change log is the current WAL-equivalent replay surface for subscriptions
- retention is bounded by a configurable event horizon (`SKEINDB_CDC_RETENTION_EVENTS`, default `4096`)
- when `from_offset` (or `Last-Event-ID`) is older than `earliest_offset - 1`, the server requires the consumer to resnapshot before resuming

## 7. SkeinQL surface

Methods:
- `cdc.subscribe_table`
- `cdc.subscribe_query`
- `cdc.poll`
- `cdc.ack`
- `cdc.pause`
- `cdc.resume`
- `cdc.close`

HTTP transport:
- `GET /api/v1/cdc/sse/{sub_id}`
- `GET /api/v1/cdc/ws/{sub_id}`

## 8. Observability

Current runtime support:
- `stats.snapshot.cdc.active_subscriptions`
- `stats.snapshot.cdc.total_lag` and `stats.snapshot.cdc.max_lag`
- `stats.snapshot.cdc.paused_subscriptions`
- `stats.snapshot.cdc.pressured_subscriptions`
- `stats.snapshot.cdc.throttle_recommended_subscriptions`
- `stats.snapshot.cdc.warn_lag` and `stats.snapshot.cdc.throttle_lag`
- `stats.snapshot.cdc.min_remaining_until_resnapshot`
- `stats.snapshot.cdc.dropped_events_total`
- `stats.snapshot.cdc.resnapshot_subscriptions`, `earliest_offset`, `latest_offset`, and retention-window metadata

## 9. Testing

- ordering preserved per stream
- offsets monotonic
- acknowledged offsets suppress redelivery for older polls
- acknowledged offsets survive restart until `cdc.close`
- `cdc.pause` / `cdc.resume` state survives restart
- `cdc.poll`, SSE, and WebSocket expose `backpressure` state transitions
- SSE resumes from `Last-Event-ID` / `from_offset`
- WebSocket resumes from `Last-Event-ID` / `from_offset`
- retained-horizon loss returns `resnapshot_required` for `cdc.poll`
- retained-horizon loss emits `event: resnapshot` for SSE reconnects
- retained-horizon loss emits a `resnapshot` control frame for WebSocket reconnects
- query dependencies expand views, set-operation branches, and CTE definitions to the real base tables used for invalidation
- closed subscriptions return `not_found`
