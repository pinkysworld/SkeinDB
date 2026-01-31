# Observability and Server Statistics

Status: Draft v0.1
Last updated: 2026-01-17

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
- p50/p95/p99 latency (if enabled)
- cache hit rates (row cache, value cache)
- coalescing hit rate (if enabled)
- in-flight query groups
- autoparameterization hit rate (if enabled)
- plan cache entries (if enabled)
- index advisor: suggestions_pending (if enabled)

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

History/replay (if enabled):
- oldest retained commit_ts
- retained history bytes
- replay exports/imports + verify failures

CDC (if enabled):
- active subscriptions
- max lag (producer_offset - consumer_offset)

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
- p50/p95/p99 over last N minutes
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

---

## 6) Backlog

- ST01: Implement basic counters + `stats.snapshot`
- ST02: Implement query fingerprint store + `stats.top_queries`
- ST03: Implement slow query log + UI
- ST04: Implement `/metrics`
- ST05: Expose storage stats (compaction, dedup, delta)
- ST06: Expose cluster stats (lag, shard placement)
