# Clustering and Scale-Out (SkeinCluster)

Status: Prototype v0.2 (control-plane + replication transport + shard placement)
Last updated: 2026-02-21

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

Minimal failover options:
- manual promotion of a replica
- optional automatic promotion if quorum is available (future)

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

---

## 5) Cluster management API (SkeinQL)

Proposed methods:

- `cluster.status`
- `cluster.nodes`
- `cluster.join_token.create`
- `cluster.node.join`
- `cluster.node.remove`
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
- `cluster.replica.promote`
- `cluster.shard.create`
- `cluster.shard.move`
- `cluster.shard.rebalance`

Replication transport implemented:
- primary node enforces write ownership per shard/global primary
- successful write RPCs are fanned out to replica nodes over HTTP RPC
- replica applies replicated writes using `x-skeindb-replication: 1`
- replication counters are exposed in `cluster.status` and `stats.snapshot.cluster`

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
- [ ] CL02: WAL streaming protocol + replica applier (full LSN stream)
- [ ] CL03: CAS object fetch protocol (ValueID pull)
- [x] CL04: replica read-only serving + lag metrics (RPC + stats snapshot exposure)
- [x] CL05: join token + node join/leave
- [x] CL06: UI cluster page (SkeinAdmin)
- [x] CL07: sharding metadata + router prototype (write ownership + shard primary checks)
- [x] CL08: shard move and rebalance (v1)
