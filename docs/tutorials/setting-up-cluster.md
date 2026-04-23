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

## 6. Production notes

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
