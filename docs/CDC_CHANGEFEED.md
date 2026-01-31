# Change Data Capture (CDC) and dependency-driven changefeeds

Status: Draft
Last updated: 2026-01-17

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
- `commit_ts` and (optional) `lsn`
- `db`, `table`
- `op`: insert | update | delete
- `pk`: primary key values
- `before` and `after` images (optional, configurable)

## 3. Streams

### 3.1 Table streams

- `cdc.subscribe_table` creates a stream for a given table.
- Optional filters (pk range, shard, columns).

### 3.2 Query streams (dependency-driven)

- `cdc.subscribe_query` subscribes to changes that might affect a prepared query.
- The server computes a dependency set for the query (tables, key ranges, or index ranges).
- Whenever a commit intersects the dependency set, the server emits a "query invalidated" event:
  - query_id
  - new_etag
  - changed_keys summary (optional)

This gives applications a direct way to update caches and UIs.

## 4. Delivery mechanisms

v1 supports:
- SSE: `GET /api/v1/cdc/sse/{sub_id}`
- Long poll: `cdc.poll` (RPC)

Later:
- WebSocket with backpressure

## 5. Exactly-once vs at-least-once

- Default delivery is at-least-once.
- Consumers ACK offsets via `cdc.ack`.
- Server retains a bounded backlog per subscription and applies backpressure.

## 6. Retention

Retention is tied to WAL retention:
- if WAL is truncated before a consumer catches up, the subscription must resnapshot.

## 7. SkeinQL surface

Methods:
- `cdc.subscribe_table`
- `cdc.subscribe_query`
- `cdc.poll`
- `cdc.ack`
- `cdc.close`

## 8. Observability

Expose:
- active_subscriptions
- lag (max_offset - consumer_offset)
- dropped_events_total

## 9. Testing

- ordering preserved per stream
- offsets monotonic
- subscriber reconnect resumes from last ack
