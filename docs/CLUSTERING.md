# Clustering and Scale-Out (SkeinCluster)

Status: Prototype v0.2 (control-plane + replication transport + shard placement)
Last updated: 2026-02-25

This document defines SkeinDB's approach to clustering.

SkeinDB starts as a single-node system, but can be expanded into a cluster for:
- higher read throughput (replicas)
- higher availability (failover)
- higher write throughput (sharding)

The design is intentionally incremental:
- **Level 1: Primary + read replicas** (simple, practical)
- **Level 2: Sharded tables** (scale writes)
- **Level 3: Elastic rebalancing** (move shards between nodes)

A key differentiator for SkeinDB clustering is that its storage is content-addressed (ValueID):
replication can transmit *references* to objects and only send missing objects on-demand.

---

## 1) Components

### 1.1 Node roles

- **Primary**: accepts writes for its shard(s), produces WAL
- **Replica**: replays WAL, serves read-only queries
- **Router (optional)**: accepts client connections and routes queries to correct primary/replica

A single executable may run in any role via CLI flags.

### 1.2 Cluster identity

Each node has a stable `node_id`.
A cluster has a stable `cluster_id`.

Nodes join clusters using a short-lived join token.

---

## 2) Level 1: Primary + Replicas

### 2.1 Replication stream (WAL shipping)

Baseline replication is WAL shipping:
- primary streams WAL records to replicas
- replica applies committed transactions in order

Replica maintains:
- last applied LSN
- lag metrics

### 2.2 CAS-assisted replication (novel improvement)

Because SkeinDB stores large values as immutable objects addressed by ValueID,
the replication stream can be optimized:

- WAL can transmit row versions referencing ValueIDs
- replica checks if it already has the referenced objects
- only missing objects are requested and transferred

See docs/CAS_REPLICATION.md for the missing-object protocol and publishable bandwidth metrics.

This avoids redundant transfer when:
- multiple replicas exist
- values are deduplicated
- delta chains share a common base

### 2.3 Read scaling

Replica can serve:
- SkeinQL queries (HTTP)
- optionally MySQL connections in read-only mode

Router can distribute reads using:
- round-robin
- least-lag
- latency-aware selection

Writes always go to the primary in Level 1.

### 2.4 Failover

Failover is **shipped** and split-brain-safe (see docs/CONFIGURATION.md → "Failure detection & failover"):
- **manual**, quorum-gated promotion (`cluster.replica.promote` — refused unless the promoter observes a majority; `force` overrides for operator recovery), and
- opt-in **automated fenced failover** (`SKEINDB_CLUSTER_AUTO_FAILOVER`): heartbeat-based failure detection, quorum write-fencing (a primary that loses its majority stops serving writes), a monotonic leadership epoch, and a Raft-style leader-election vote round — **whole-cluster and per-shard**, each an independent replication group with its own quorum/epoch/election.
- **Data-safe election (Raft log matching).** The election prefers the **most up-to-date** replica by replication progress (`applied_ops`, propagated on every heartbeat), and a voter **refuses any candidate less caught up than itself**. Because a committed write reaches a majority and a winner needs a majority of votes, the elected primary holds every committed write — failover cannot lose acknowledged data. (Bounds and caveat: see 2.5.)

### 2.5 Replication: self-healing today, the consensus roadmap ahead

Baseline replication is best-effort primary→replica fan-out: on a write the primary applies locally and re-issues the same RPC to each replica (`x-skeindb-replication: 1`), carrying a causality token for read-your-writes ordering; the replica re-executes it. On its own, fan-out has no ordering and no recovery — a replica that misses a write during a transient failure would diverge with nothing to backfill it. **Self-healing replication (below) closes that gap.**

**Self-healing replication (shipped).** Every replicated write now carries a primary-assigned log position `(term, seq)` (`x-skeindb-replication-seq`), and the primary keeps a bounded in-memory **op-log ring buffer** (`REPLICATION_LOG_BUFFER_CAP`, default 4096) of recent ops. On the receiving side a replica tracks its **contiguous applied position** (`last_applied_term`/`last_applied_seq`) and, for each incoming op, either:

- **applies** it if it is the next in order (or the first of a newer leadership term),
- treats it as a **duplicate** (idempotent no-op) if already applied — so re-delivery is always safe, or
- recognises a **gap** (a missing op below it) and skips it, leaving its position unchanged.

A replica's background loop then **pulls** whatever it is missing from the primary via `cluster.replication.fetch{after_term, after_seq}` and applies it in order — so a replica that fell behind (or joined late) **automatically catches up**. If a replica has fallen further behind than the buffer still retains, the primary returns `resync_required` and that replica must be re-synced from a snapshot. Each node's position is visible in `cluster.replication.status` and `cluster.failover.status`.

This corresponds to slices 1–3 of the roadmap below (op identity → gap-aware ordered apply → leader-driven catch-up). The failover **data-safety** guarantee (2.4) sits on top: `applied_ops` is monotonic, and with one primary per term positions share a common prefix, so "most-caught-up wins + a voter refuses a less-caught-up candidate" preserves every committed write.

**Remaining: true `(term, index)` consensus.**

4. **Commit index (majority-ack).** A write becomes "committed" once a majority has acked its `(term, seq)`; only committed ops are electable, the election compares true `(term, index)` positions, and a leadership change reconciles divergent log tails rather than adopting best-effort. Closes the count-vs-`(term, index)` caveat and completes the move from best-effort self-healing to full log-replication consensus. Sequenced as a dedicated project (see also the per-database lock-sharding finding in docs/PERFORMANCE.md §4b).

> **Scope note.** Slices 1–3 make steady-state replication self-healing (the common production case: a replica lags on a blip and recovers). Reconciling logs across a *leadership change* is best-effort until slice 4 — a replica adopts a new leader at the op that first reaches it, and if the buffer cannot cover the gap it falls back to a snapshot re-sync.

---

## 3) Level 2: Sharding

Sharding is opt-in and explicit.

### 3.1 Shard keys

A sharded table defines:
- shard key (usually the primary key or a chosen column)
- shard function:
  - hash-based: consistent hashing
  - range-based: key ranges

### 3.2 Routing rules

- single-shard queries route to one primary
- multi-shard queries are limited in v1 (or executed as scatter-gather)

### 3.3 Transaction scope

In Level 2 (v1), transactions are single-shard for simplicity.
Cross-shard transactions are a future extension.

---

## 4) Level 3: Elastic rebalancing

Rebalancing moves shards between nodes.

Novel opportunity:
- content-addressed ValueStore allows "object set" transfer
- shard move can transfer only missing ValueIDs
- reduces time and bandwidth for rebalance

Current prototype coverage:
- the source node enumerates a shard-scoped object manifest from live row versions
- the destination node preflights the manifest with `objects.need`
- non-dry-run `cluster.shard.move` / `cluster.shard.rebalance` calls pull only missing objects via `objects.pull` before changing primary placement
- move and rebalance responses include manifest/progress summaries so operators can report object counts, bytes, and pull outcomes

---

## 5) Cluster management API (SkeinQL)

Proposed methods:

- `cluster.status`
- `cluster.nodes`
- `cluster.join_token.create`
- `cluster.node.join`
- `cluster.node.remove`
- `cluster.node.leave`
- `cluster.replica.promote`

For sharding:
- `cluster.shard.create`
- `cluster.shard.move`
- `cluster.shard.rebalance`

Implemented in this build:
- `cluster.status`
- `cluster.nodes`
- `cluster.join_token.create`
- `cluster.node.join`
- `cluster.node.remove`
- `cluster.node.leave`
- `cluster.replica.promote`
- `cluster.shard.create`
- `cluster.shard.move`
- `cluster.shard.rebalance`

Replication transport implemented:
- primary node enforces write ownership per shard/global primary
- successful write RPCs are fanned out to replica nodes over HTTP RPC
- replica applies replicated writes using `x-skeindb-replication: 1`
- replicated table/view writes also carry `x-skeindb-replication-causality` so replicas can retain the upstream dependency watermark without imposing a global total order
- replication counters plus the merged applied causality watermark are exposed in `cluster.status` and `stats.snapshot.cluster`
- graceful shutdown (`Ctrl+C`, `SIGTERM`, or `system.shutdown`) marks the local node offline and sends best-effort `cluster.node.leave` notifications to online peers

---

## 6) UI requirements (SkeinAdmin)

Cluster settings section should include:
- topology graph
- node list with role/health
- replication lag
- promote replica
- add/remove node
- shard placement view

---

## 7) Backlog

- [x] CL01: node_id + cluster_id plumbing
- [~] CL02: replication — best-effort primary→replica RPC fan-out **plus self-healing catch-up** (op-log `(term, seq)`, idempotent/gap-aware apply, `cluster.replication.fetch` pull); full commit-index consensus is the remaining §2.5 item
- [ ] CL03: CAS object fetch protocol (ValueID pull)
- [x] CL04: replica read-only serving + lag metrics (RPC + stats snapshot exposure)
- [x] CL05: join token + node join/leave
- [x] CL06: UI cluster page (SkeinAdmin)
- [x] CL07: sharding metadata + router prototype (write ownership + shard primary checks)
- [x] CL08: shard move and rebalance (v1)
- [x] CL09: automated fenced failover — quorum fencing, leadership epoch, Raft-style vote round (whole-cluster + per-shard), **data-safe election** (log-matching by `applied_ops`)
- [~] CL10: self-healing replication (§2.5) — **shipped**: idempotent apply, gap-aware ordered apply, leader-driven catch-up (slices 1–3). Remaining: commit-index / true `(term, index)` consensus (slice 4)
