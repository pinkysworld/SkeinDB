# Configuration

SkeinDB is designed to run as a **single executable** with configuration primarily via CLI flags.

The goal is a low-friction deployment model:
- copy the binary
- pick ports
- pick a data directory
- run

> The exact flags may evolve; this document describes the intended interface and the current scaffold behavior.

---

## CLI

```text
skeindb serve [OPTIONS]

OPTIONS:
  --data <path>      Data directory (WAL, snapshots, metadata)
  --http <port>      HTTP port (SkeinQL + admin console)
  --mysql <port>     MySQL protocol port (compatibility surface)
  --bind <ip>        Bind address (default 127.0.0.1)
```

### Examples

Run on ports 8080/3306:

```bash
./skeindb serve --data ./data --http 8080 --mysql 3306
```

Run HTTP-only:

```bash
./skeindb serve --data ./data --http 8080 --mysql 0
```

---

## HTTP services

When enabled, the HTTP listener serves:
- `POST /api/v1/rpc` SkeinQL JSON-RPC
- `GET /api/v1/q/:query_id` prepared query execution (ETag validators)
- `GET /admin` (SkeinAdmin)
- `GET /metrics` (Prometheus-style counters)

---

## Data directory layout (prototype)

The in-memory prototype persists a small amount of metadata.

Planned layout (subject to change as the WAL/segment formats are implemented):

```text
<data>/
  wal/
  snapshots/
  meta/
```

---

## Running behind a reverse proxy

SkeinDB is compatible with standard reverse proxies (Apache, Nginx, IIS) because the control plane is HTTP.

Recommended settings:
- keep HTTP/2 enabled where possible
- use TLS termination at the proxy
- enable gzip/brotli compression
- set cache headers for prepared-query GET endpoints (`/api/v1/q/...`)

---

## Clustering settings

Clustering is managed via SkeinQL (`cluster.*`) and is designed to be configured without external orchestration.

See:
- `docs/CLUSTERING.md`
