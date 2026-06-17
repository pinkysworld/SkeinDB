# Observability and Server Statistics

Status: Baseline implemented, per-fingerprint histogram drill-down, CDC pressure telemetry, basic operator alerts, settings-backed alert routing, `stats.snapshot`-driven HTTP(S) webhook delivery, and configurable escalation automation (cooldown re-fire + warning->critical severity ladder) landed; non-HTTP sinks and retry policies remain backlog
Last updated: 2026-06-17

This document defines the observability surface of SkeinDB:
- server load and resource stats
- query and storage statistics
- background task visibility (compaction, snapshots, replication)
- metrics export

The goal is to make SkeinDB operable at higher loads **without** external tooling,
while still integrating cleanly with standard monitoring stacks.

---

## 1) Principles

1. **Low overhead by default**
   - counters should be cheap (atomic increments)
   - sampling is used for expensive stats (latency histograms)

2. **Two audiences**
   - humans (SkeinAdmin dashboards)
   - machines (metrics endpoints / exporters)

3. **Useful out of the box**
   - no Prometheus required to see basic health
   - but Prometheus-compatible metrics should be available

---

## 2) Stats data model

### 2.1 Instantaneous system snapshot

A `stats.snapshot` should include:
- process uptime
- CPU usage (process and/or system)
- memory RSS, heap
- open file descriptors / handles
- disk usage for data directory
- network bytes in/out

### 2.2 Database workload snapshot

- active sessions
- active queries
- QPS (queries/sec) recent window
- TPS (transactions/sec)
- commit rate
- average latency plus `p50` / `p95` / `p99` latency over the recent query sample window
- cache hit rates (row cache, value cache)
- coalescing hit rate (if enabled)
- in-flight query groups
- autoparameterization hit rate (if enabled)
- plan cache entries (if enabled)
- index advisor: suggestions_pending (if enabled)

Current runtime surface in `stats.snapshot.query`:
- `tracked_calls`
- `fingerprints`
- `recent_samples`
- `slow_count`
- `avg_latency_ms`
- `latency_ms.p50`
- `latency_ms.p95`
- `latency_ms.p99`
- `etag_hits`
- `coalesced`

Current synthesized alert surface in `stats.snapshot.alerts`:
- overall `status`
- `summary.critical`
- `summary.warning`
- `summary.total`
- optional `routing.configured`
- optional `routing.routed_alerts`
- optional `routing.matched_routes`
- optional `routing.delivery.delivered`
- optional `routing.delivery.suppressed`
- optional `routing.delivery.failed`
- optional `routing.delivery.unsupported`
- per-item `code`, `severity`, `component`, `panel`, `title`, `summary`, and `action`
- optional per-item `routes[].id`
- optional per-item `routes[].targets`
- optional per-item `routes[].delivery.delivered`
- optional per-item `routes[].delivery.suppressed`
- optional per-item `routes[].delivery.failed`
- optional per-item `routes[].delivery.unsupported`

Current routing config surface:
- `settings.set { "observability.alert_routes": [...] }`
- each route can declare `id`, `min_severity`, `components`, `panels`, `codes`, and `targets`
- `stats.snapshot.alerts` resolves matching routes against the current synthesized alerts
- `http://` and `https://` targets receive a JSON `POST` the first time a matching alert is observed in `stats.snapshot`
- identical active alerts are suppressed on subsequent `stats.snapshot` calls until the alert clears
- non-HTTP targets are still annotated in the snapshot but are not delivered externally yet

Current drill-down surface in `stats.query_fingerprint_latency`:
- bounded recent per-fingerprint latency samples
- `p50_ms`, `p95_ms`, and `p99_ms`
- millisecond histogram buckets with `overflow_count`
- optional filtering by exact fingerprint for operator drill-down

### 2.3 Storage engine snapshot

- WAL size and append rate
- checkpoint age
- compaction state (running? stage?)
- compaction throughput
- compaction queue length + stall/backpressure events
- LSM levels sizes
- ValueStore:
  - unique values
  - logical bytes vs physical bytes
  - dedup ratio
  - encryption mode and encrypted objects count (if enabled)
  - delta chain depth stats (if DELTA enabled)

Current runtime surface in `stats.snapshot.storage`:
- `wal_bytes`
- `dedup_ratio`
- `logical_bytes`
- `unique_bytes`
- `duplicate_bytes`
- `unique_values`
- `interned_values`
- `total_rows`
- `total_tables`
- `disk_bytes`
- `core_lsm_active`
- `wal_recovery_pending`
- `wal_replayed_mutations`
- `wal_truncated_torn_tail`
- `last_checkpoint_ts_ms`
- `checkpoint_dirty_tables`

Storage recovery note:
- the live engine still serves row snapshots, but restart safety now bridges through
  `MANIFEST.log` + `wal-000001.log`
- table snapshots persist their latest applied storage WAL LSN
- startup replays only newer committed row mutations and truncates torn WAL tails
- `checkpoint_dirty_tables > 0` means there are committed WAL-backed table mutations
  that have not yet been cleared by a successful durable persist/checkpoint cycle

History/replay (if enabled):
- oldest retained commit_ts
- retained history bytes
- replay exports/imports + verify failures

CDC (if enabled):
- active subscriptions
- max lag (producer_offset - consumer_offset)

Current runtime surface in `stats.snapshot.cdc`:
- `active_subscriptions`
- `table_subscriptions`
- `query_subscriptions`
- `total_lag`
- `max_lag`
- `paused_subscriptions`
- `pressured_subscriptions`
- `throttle_recommended_subscriptions`
- `warn_lag`
- `throttle_lag`
- `min_remaining_until_resnapshot`
- `resnapshot_subscriptions`
- `earliest_offset`
- `latest_offset`
- `retained_events`
- `retention_limit`
- `dropped_events_total`

### 2.4 Cluster snapshot (if enabled)

- node role (primary/replica/router)
- replication lag (LSN difference)
- missing object fetch stats (CAS replication)
- CAS object hit rate and bytes_saved
- shard placement summary

---

## 3) Export surfaces

### 3.1 SkeinQL methods

- `stats.snapshot` -> JSON summary for dashboards
- `stats.top_queries` -> top by total time / p95 / rows
- `stats.query_fingerprint_latency` -> per-fingerprint p50/p95/p99 plus bounded latency histogram buckets
- `stats.slow_queries` -> recent slow query log
- `stats.storage` -> compaction + disk + dedup
- `stats.cluster` -> node + shard view

### 3.2 HTTP metrics endpoint (Prometheus-style)

Optional endpoint:
- GET `/metrics`

Design notes:
- keep metric names stable
- include labels:
  - db, table, node_id, shard_id, role

---

## 4) Query statistics

### 4.1 Query fingerprinting

- normalize query text (or SkeinIR form)
- compute `query_fingerprint` (hash)
- track:
  - count
  - total_time
  - rows_returned
  - bytes_returned

### 4.2 Sampling

To keep overhead low:
- always count
- sample timing at a configurable rate (e.g., 1/100)

---

## 5) UI requirements (SkeinAdmin)

The "Server Load & Stats" section should include:

1) **Overview**
- CPU, RAM, disk, network
- QPS/TPS
- active sessions

2) **Latency**
- average latency plus p50/p95/p99 over the recent query sample window
- slow query list

3) **Storage**
- WAL growth
- compaction progress
- dedup ratio
- snapshot sizes

4) **Cluster** (if enabled)
- node list with health
- replication lag
- shard placement

Delivered baseline in SkeinAdmin:
- overview cards for cache, average latency, and p95 tail latency
- overview `Operational Alerts` card synthesized from current query, CDC, and compaction telemetry
- active-session summary text with average, p95, and p99 latency
- telemetry panel table for per-fingerprint latency histograms with p95/p99 drill-down
- CDC panel summary cards for runtime subscription counts, lag, retention horizon, and dropped-event totals

Delivered operator routing baseline:
- persisted alert-route configuration via `observability.alert_routes`
- per-alert matched route IDs/targets in `stats.snapshot.alerts.items[*].routes`
- top-level route summary counters in `stats.snapshot.alerts.routing`
- HTTP(S) route targets receive JSON `POST` delivery once per active alert while `stats.snapshot` is being evaluated
- escalation automation via `observability.alert_escalation`:
  - `refire_after_secs` (default `0`, disabled): re-deliver an already-active alert once this many seconds have elapsed since its last delivery instead of suppressing it for the lifetime of the alert
  - `escalate_after_secs` (default `0`, disabled): promote a `warning` alert to `critical` once it has been continuously active for this many seconds; escalated items gain `escalated: true` / `escalated_from`, re-match routes at the higher severity, and update the snapshot `summary`/`status`
  - `stats.snapshot.alerts.routing.delivery.escalated` reports how many alerts were escalated in the current evaluation
- non-HTTP sinks and per-target retry policies remain backlog work

---

## 6) Backlog

- ST01: Implement basic counters + `stats.snapshot`
- ST02: Implement query fingerprint store + `stats.top_queries`
- ST03: Implement slow query log + UI
- ST04: Implement `/metrics`
- ST05: Expose storage stats (compaction, dedup, delta)
- ST06: Expose cluster stats (lag, shard placement)
