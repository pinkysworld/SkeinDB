# SkeinQL-over-QUIC

Status: Hardened runtime surface with comparative benchmark evidence (2026-05-15)

This document describes the current QUIC transport for SkeinQL (R09). It is
intentionally small and focused on framing + stream mapping so the runtime can
evolve without breaking the core HTTP RPC.

## Goals
- Multiplexed queries without head-of-line blocking.
- One request per bidirectional QUIC stream.
- Keep the wire format close to the existing JSON RPC envelope.

## Framing
Each QUIC stream carries exactly one request and (optionally) one response.
The payload is length-prefixed JSON.

```
frame := u32_be_len | json_bytes
```

- `u32_be_len` is a 4-byte big-endian length of `json_bytes`.
- `json_bytes` is UTF-8 JSON.
- Maximum frame size is 16 MiB in the current prototype.

## Request / Response
- **Request**: a `RpcRequest` JSON object (see `docs/SKEINQL.md`), or an
  envelope with transport metadata.
- **Response**: a `RpcResponse` JSON object.
- **Notifications** (`id` is null or omitted) produce **no response frame**.

### Envelope with Transport Metadata
```json
{
  "request": { "skeinql": "1.0", "id": "r1", "method": "system.ping", "params": {} },
  "meta": { "rtt": "0rtt", "read_only": true }
}
```

`meta` fields:
- `rtt`: `"0rtt"` or `"1rtt"` (default `"1rtt"`).
- `read_only`: hint that the request is read-only.

If `rtt` is `"0rtt"`, the server enforces a read-only method allowlist.

## Stream Mapping
- Clients open a new bidirectional stream for each request.
- Servers read a single frame, emit a single response frame (if any), and
  finish the stream.
- Multiple streams may be active concurrently on the same connection.

## Prepared Queries
Prepared queries are ordinary RPC methods on the QUIC transport:
- `query.prepare` returns a `query_id`.
- Clients use new streams to call `query.execute_prepared` with the `query_id`.
- `if_none_match` provides ETag validation semantics for cache-aware clients.

## TLS / Certificates
QUIC always uses TLS 1.3. The prototype requires explicit cert/key paths:

```
skeindb serve --quic 4433 --quic-cert ./cert.pem --quic-key ./key.pem
```

## Auth Placeholder
QUIC does not carry HTTP headers. If `SKEINDB_TOKEN` is set, QUIC requests are
rejected until a QUIC auth envelope is defined.

## 0-RTT
0-RTT is represented by `meta.rtt="0rtt"` in the envelope. The prototype
enforces a read-only allowlist for these requests to prevent replayed writes.
Future work will add handshake-level 0-RTT enablement and stricter replay
protection.

## Test Evidence

The R09 runtime surface is covered by `crates/skeindb/tests/quic_rpc.rs`:

- `quic_rpc_ping_roundtrip` verifies the Quinn-backed listener and JSON-RPC framing.
- `quic_prepared_query_roundtrip` verifies prepared-query handles over QUIC streams.
- `quic_zero_rtt_rejects_write` verifies the read-only 0-RTT guard.
- `quic_connection_migration_rebind` and `r09_quic_concurrent_multi_stream_rpcs` verify socket rebind and continued RPC success.

Comparative transport benchmarking is covered by the new CLI harness:

```bash
skeindb transport-bench \
  --http-url http://127.0.0.1:8080 \
  --mysql-port 3306 \
  --quic-port 4433 \
  --quic-cert ./cert.pem \
  --quic-server-name localhost \
  --concurrency 8 \
  --requests 64 \
  --sql "SELECT 1 AS one"
```

`skeindb transport-bench` reuses the same `sql.exec` JSON-RPC request bytes for
HTTP/2 and QUIC, issues `COM_QUERY` for the same SQL on MySQL/TCP, and reports
per-transport min/p50/p95/p99/max/mean latency in nanoseconds. The live
comparison path is locked by `crates/skeindb/tests/transport_bench.rs::transport_bench_reports_http2_quic_and_mysql`.
