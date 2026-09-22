# SkeinDB Two-Node CAS HTTP Wire Validation

Format: `skein.cas_http_wire.runtime.v1`  
Rows per scenario: **300**  
Seed: **20260922**  
Pull batch size: **32**

A transparent TCP proxy counts the exact TCP payload bytes used by the production objects.pull path. CR09 keeps the request JSON-compatible but prefers a binary fetch response, with automatic fallback to the legacy JSON-RPC objects.fetch path. Each scenario compares a zero-overlap destination with a destination pre-seeded at the configured CAS overlap.

| Scenario | Baseline HTTP bytes | CAS HTTP bytes | HTTP bytes saved | Object bytes saved | Baseline connections | CAS connections | Baseline wire / materialized value | CAS wire / materialized value | Second-pull HTTP bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| low_redundancy_high_churn | 34361 | 25739 | 25.1% | 25.1% | 1 | 1 | 0.61x | 0.61x | 0 |
| balanced | 18426 | 7371 | 60.0% | 60.0% | 1 | 1 | 0.62x | 0.62x | 0 |
| high_redundancy_low_churn | 4059 | 807 | 80.1% | 86.7% | 1 | 1 | 0.61x | 0.91x | 0 |

## What the byte counter includes

- HTTP request line and request headers
- JSON request bodies containing hexadecimal ValueIDs
- HTTP status line and response headers
- CR09 binary response framing and canonical transfer-entry bytes
- all request JSON syntax and metadata carried over the loopback TCP stream
- the legacy JSON-RPC `objects.fetch` response remains compatibility-tested separately and is used as an automatic fallback for older nodes

## What it excludes

- Ethernet, IP, and TCP packet headers
- retransmission accounting below the TCP stream
- TLS framing, because the CI experiment uses plain loopback HTTP
- latency and throughput claims

The report therefore measures exact **HTTP-over-TCP payload bytes** seen by the transparent proxy, not packet-capture bytes on a physical network. `ValueStore object bytes` are materialized-value bytes from runtime counters; canonical delta transfer entries can legitimately be smaller, so wire/materialized-value ratios below 1 are expected and are not negative protocol overhead.
