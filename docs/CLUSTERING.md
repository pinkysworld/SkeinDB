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

### 2.5 Replication implementation status & the consensus roadmap

**Sections 2.1–2.2 describe the _target_ design; the replication actually implemented today is best-effort primary→replica fan-out.** Being precise about the gap matters, because it bounds what the failover guarantee means.

- On a write, the primary applies locally and then re-issues the same RPC to each replica (`x-skeindb-replication: 1`), carrying a causality token for read-your-writes ordering. The replica re-executes the RPC and increments `applied_ops`.
- There is **no ordered log, no per-entry index, no de-duplication/idempotency, no acknowledgement-based commit index, and no catch-up.** A replica that misses a write during a transient failure (counted in `failed_ops`) **diverges permanently** — nothing backfills it.

Consequences:
- The shipped **failover data-safety** guarantee (2.4) is sound under the current single-primary model: `applied_ops` is a monotonic count of applied replicated writes, and with one primary per term those counts share a common prefix, so "most `applied_ops` wins + a voter refuses a less-caught-up candidate" really does preserve every committed write. Its caveat — that a count is not a true `(term, index)` log position — only bites once logs can genuinely diverge, which requires the work below.
- A diverged replica is correctly **refused** by log-matching (which protects data), but that replica is then dead weight until manually rebuilt. **Self-healing replication is the real production gap**, not the theoretical `(term, index)` soundness.

**Roadmap to a true replicated log / self-healing replication.** Strictly dependency-ordered. Note that the early bricks carry _no standalone user-visible value_ — the payoff (a replica that fell behind automatically recovers) only lands at slice 3, which is why this is scoped as one deliberate project rather than shipped piecemeal:

1. **Idempotent apply (op identity).** Stamp each replicated write with a `(term, seq)` assigned by the primary; the replica records its high-water position and treats a re-delivered op as a success no-op. Prerequisite for any re-send. *(Additive; behaviour-preserving today because no duplicates occur, so its value is latent.)*
2. **Ordered, gap-aware apply.** The replica tracks a contiguous applied index and detects a gap (a dropped op) instead of silently advancing past it — turning `applied_ops` into a trustworthy contiguous position. *(Not independently shippable: without slice 3 a replica stalls on the first gap.)*
3. **Leader-driven catch-up.** The primary keeps a bounded buffer of recent ops (falling back to a snapshot/WAL segment) and re-ships missing ops, in order, to any replica reporting a lower position via heartbeat. **This is the outcome: replicas self-heal after transient failures.**
4. **Commit index (majority-ack).** A write is "committed" once a majority acks its `(term, seq)`; only committed ops are electable, and the election compares true `(term, index)` positions. Closes the divergent-log caveat and upgrades leader election toward full log-replication consensus.

Slices 1–2 are additive and low-risk but latent; slice 3 is where the value is and is a substantial change to the write path (the highest-risk surface); slice 4 completes the consensus story. Like the per-database lock-sharding finding in docs/PERFORMANCE.md §4b, this is sequenced here as a dedicated project rather than begun as a partial rewrite.

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
- [~] CL02: replication — **best-effort primary→replica RPC fan-out shipped** (causality token for read-your-writes); ordered LSN/log stream is the roadmap in §2.5, not yet built
- [ ] CL03: CAS object fetch protocol (ValueID pull)
- [x] CL04: replica read-only serving + lag metrics (RPC + stats snapshot exposure)
- [x] CL05: join token + node join/leave
- [x] CL06: UI cluster page (SkeinAdmin)
- [x] CL07: sharding metadata + router prototype (write ownership + shard primary checks)
- [x] CL08: shard move and rebalance (v1)
- [x] CL09: automated fenced failover — quorum fencing, leadership epoch, Raft-style vote round (whole-cluster + per-shard), **data-safe election** (log-matching by `applied_ops`)
- [ ] CL10: self-healing replication (§2.5 slices 1–4): idempotent apply → gap-aware ordered apply → leader-driven catch-up → commit-index / true `(term, index)` consensus
