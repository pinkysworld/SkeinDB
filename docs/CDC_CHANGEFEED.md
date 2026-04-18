# Change Data Capture (CDC) and dependency-driven changefeeds

Status: Partial implementation
Last updated: 2026-04-16

Current runtime baseline:
- `cdc.subscribe_table` creates table subscriptions over the RPC API.
- `cdc.subscribe_query` creates dependency-driven subscriptions for prepared queries and emits invalidation events with the current query ETag.
- `cdc.poll` reads from the retained persisted change log and returns `earliest_offset` / `latest_offset` plus `resnapshot_required` metadata when a consumer falls behind the retained horizon.
- `GET /api/v1/cdc/sse/{sub_id}` streams the same subscription events over SSE, with replay from the retained change log, bounded batch delivery, reconnect via `Last-Event-ID` or `from_offset`, and a terminal `resnapshot` control event when the reconnect cursor falls behind retention.
- `cdc.ack` advances an in-memory consumer cursor per subscription.
- `cdc.close` removes the subscription handle.
- SkeinAdmin now includes a dedicated CDC page for `cdc.subscribe_table` / `cdc.poll` / `cdc.ack` / `cdc.close`, with session-local lag visualization derived from `next_offset - acked_offset`.
- WebSocket delivery and durable consumer cursors are still open backlog items.

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
- row-level table events persist `commit_ts_ms` and `lsn = seq`
- query invalidation events reuse the triggering table event metadata and override `op = "invalidate"`

## 3. Streams

### 3.1 Table streams

- `cdc.subscribe_table` creates a stream for a given table.
- Current runtime scope: full-table subscriptions only.
- Optional filters (pk range, shard, columns) remain planned.

### 3.2 Query streams (dependency-driven)

- `cdc.subscribe_query` subscribes to changes that might affect a prepared query.
- The server computes a dependency set for the query (tables, key ranges, or index ranges).
- Whenever a commit intersects the dependency set, the server emits a "query invalidated" event:
  - query_id
  - new_etag
  - changed_keys summary (optional)

Current runtime scope:
- dependency sets are conservative and table-based, reusing the same prepared-query dependency metadata used for query ETags
- invalidation events are delivered through `cdc.poll` with `op = "invalidate"`, the triggering `db` / `table`, and optional `query_id` / `etag` fields
- subscriptions are bound to a prepared `query_id` plus its positional args

This gives applications a direct way to update caches and UIs.

## 4. Delivery mechanisms

Current runtime support:
- Long poll: `cdc.poll` (RPC)
- SSE: `GET /api/v1/cdc/sse/{sub_id}`
  - emits the same JSON event payloads as `cdc.poll`
  - replays from `?from_offset=<seq>` or `Last-Event-ID: <seq>`
  - drains the retained change log in bounded batches so slow consumers can reconnect without losing events inside the retained horizon
  - emits `event: resnapshot` with recovery metadata when the reconnect cursor falls behind retention

Planned follow-ons:
- WebSocket with backpressure

## 5. Exactly-once vs at-least-once

- Default delivery is at-least-once.
- Consumers ACK offsets via `cdc.ack`.
- Current runtime tracks acked offsets in memory per subscription and suppresses redelivery for older offsets.
- SSE reconnects are driven by event `id = seq`; clients can resume by supplying `Last-Event-ID` or `from_offset`.
- When a consumer falls behind the retained horizon, `cdc.poll` returns `resnapshot_required = true` and SSE emits a `resnapshot` control event instead of replaying a partial stream.
- Durable cursors and richer backpressure policies remain follow-on work.

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
- `cdc.close`

HTTP transport:
- `GET /api/v1/cdc/sse/{sub_id}`

## 8. Observability

Expose:
- active_subscriptions
- lag (max_offset - consumer_offset)
- dropped_events_total

## 9. Testing

- ordering preserved per stream
- offsets monotonic
- acknowledged offsets suppress redelivery for older polls
- SSE resumes from `Last-Event-ID` / `from_offset`
- retained-horizon loss returns `resnapshot_required` for `cdc.poll`
- retained-horizon loss emits `event: resnapshot` for SSE reconnects
- closed subscriptions return `not_found`
