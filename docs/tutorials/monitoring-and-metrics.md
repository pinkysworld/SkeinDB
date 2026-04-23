# Monitoring and metrics

This guide shows the fastest way to inspect a live SkeinDB node: health endpoint, Prometheus metrics, SkeinQL stats, and the built-in admin UI.

Prerequisite: [Quickstart](quickstart.html) completed.

## 1. Start the server

```bash
cargo run -- serve --data ./data --http 8080 --mysql 3306 --pg 5432
```

## 2. Check basic liveness

```bash
curl -s http://127.0.0.1:8080/health
```

Use this for load balancers and simple smoke checks.

## 3. Inspect Prometheus-style metrics

SkeinDB exposes a plain metrics endpoint at `GET /metrics`:

```bash
curl -s http://127.0.0.1:8080/metrics | head -40
```

Minimal Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: skeindb
    static_configs:
      - targets: ["127.0.0.1:8080"]
```

## 4. Pull a runtime stats snapshot over SkeinQL

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{"skeinql":"1.0","id":1,"method":"stats.snapshot","params":{}}' \
  | jq '.result | {uptime_s, qps, tps, sessions, process, storage, background}'
```

At the current baseline, the snapshot includes:

- uptime
- active and total sessions
- QPS and TPS
- process CPU and RSS
- storage metrics such as `wal_bytes` and `dedup_ratio`
- background task status

## 5. Watch the same data in SkeinAdmin

Open:

```text
http://127.0.0.1:8080/admin
```

The most useful panels for day-to-day visibility are:

- **Overview** for live health and headline server numbers
- **Telemetry** for metrics-oriented summaries
- **Cluster** when you are running replicas or shards
- **Time Travel & Replay** and **Forensics** for operational investigations

## 6. What to watch first

For a single node, start with these checks:

1. `wal_bytes` rising steadily without checkpoints or compaction progress.
2. `dedup_ratio` dropping sharply after a workload change.
3. CPU or RSS growth that does not come back down after the workload quiets.
4. Background tasks stuck in a non-idle state.

## Next

- [Observability](observability.html)
- [Admin console tour](admin-tour.html)
- [Setting up a 3-node cluster](setting-up-cluster.html)