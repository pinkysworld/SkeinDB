# Setting up a 3-node cluster

This tutorial brings up a **3-node SkeinDB cluster** on one machine using different ports. The same commands work across three hosts — only the bind addresses change.

Prerequisite: [Quickstart](quickstart.html) completed, `skeindb` binary available on `$PATH`.

## 1. Start the first node (seed)

```bash
skeindb serve \
  --data ./data/n1 \
  --http 8001 \
  --mysql 3301 \
  --node-id n1 \
  --cluster-bind 127.0.0.1:7001
```

The first node generates a **cluster id** and a **join token** on first boot. Copy the join token from the startup log (or from `./data/n1/cluster/join_token.txt`).

```text
cluster id: 8c2f…a9
join token: jt_a3d8c1…
```

## 2. Join two more nodes

On the same machine (different ports):

```bash
skeindb serve \
  --data ./data/n2 \
  --http 8002 \
  --mysql 3302 \
  --node-id n2 \
  --cluster-bind 127.0.0.1:7002 \
  --cluster-join 127.0.0.1:7001 \
  --cluster-join-token jt_a3d8c1…
```

```bash
skeindb serve \
  --data ./data/n3 \
  --http 8003 \
  --mysql 3303 \
  --node-id n3 \
  --cluster-bind 127.0.0.1:7003 \
  --cluster-join 127.0.0.1:7001 \
  --cluster-join-token jt_a3d8c1…
```

## 3. Verify the topology

```bash
curl -s -XPOST http://127.0.0.1:8001/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"cluster.topology","params":{}}' | jq
```

Or open `http://127.0.0.1:8001/admin` → **Cluster** panel. You should see three nodes and one primary.

## 4. Write on the primary, read on a replica

```bash
# Write via primary (n1)
curl -s -XPOST http://127.0.0.1:8001/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"schema.create_database","params":{"db":"shop"}}'

# Read via replica (n2)
curl -s -XPOST http://127.0.0.1:8002/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"cluster.topology","params":{}}'
```

Replicas accept read queries but forward writes. RPC fanout is recursion-suppressed, so cluster-wide operations are exactly-once.

## 5. Simulate a failure

Stop `n1`:

```bash
# Ctrl+C on n1, or:
skeindb admin cluster.demote --node-id n1 --http http://127.0.0.1:8002
```

Promote a replica from the admin UI or via SkeinQL:

```bash
curl -s -XPOST http://127.0.0.1:8002/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"cluster.promote","params":{"node_id":"n2"}}'
```

## 6. Automated fenced failover (optional)

By default, promoting a replica after the primary fails is a manual step (and is quorum-gated — a promotion is refused unless the promoting node still sees a majority of the cluster, so a minority partition can't create a second primary). To let the cluster fail over on its own, start every node with:

```bash
SKEINDB_CLUSTER_AUTO_FAILOVER=1 skeindb serve --data ./n1 --http 8001 ...
```

With this enabled, each node runs a background tick that:

1. **Heartbeats** its peers so the cluster keeps a live health view (a node unseen for `SKEINDB_CLUSTER_NODE_TIMEOUT_MS`, default 15s, is considered offline).
2. **Fences** itself if it is the primary but has lost quorum — it refuses writes (clients get a `fenced` error) so it can't diverge from the new primary the majority side will elect.
3. **Elects** a new primary on the majority side: the freshest online replica requests votes from its peers (`cluster.request_vote`) and promotes itself only if a **majority** grant it. Each node votes at most once per election term, so at most one candidate can win — no split-brain.

Watch the failover decision live (read-only, safe to poll):

```bash
curl -s -XPOST http://127.0.0.1:8001/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"cluster.failover.status","params":{}}'
# → primary_healthy, recommended_candidate, leadership_epoch, and a quorum block per node
```

> **Run 3+ nodes.** Quorum is a majority, so a 2-node cluster loses write availability when either node is down (neither side is a majority). Three nodes tolerate one failure; five tolerate two.

Sharded clusters fail over **per shard** — each shard is its own replication group. Watch every shard's readiness with `cluster.shard.failover.status`, and promote a shard's replica with `cluster.replica.promote` + a `shard_id` (now quorum-gated against the shard's own node set, and the shard primary is write-fenced when it loses that quorum). Fully self-driving per-shard election is being rolled out on top of these primitives. See [Configuration → Automated fenced failover](configuration.html#automated-fenced-failover-opt-in).

## 7. Production notes

- Put each node on its own host with a fixed `--cluster-bind` that is reachable from the other nodes.
- Rotate the join token regularly: `cluster.rotate_join_token`.
- Use **separate networks** for `--http` (applications) and `--cluster-bind` (inter-node).
- See [Clustering](clustering.html) for shard placement, replication factors, and rolling upgrade procedure.
- See [Observability](observability.html) to wire Prometheus + Grafana.

## Next

- [Clustering reference](clustering.html)
- [Observability](observability.html)
- [Audit WAL](audit-wal.html) — tamper-evident audit chain across nodes.
- [CAS replication](cas-replication.html) — dedup-aware replication internals.
