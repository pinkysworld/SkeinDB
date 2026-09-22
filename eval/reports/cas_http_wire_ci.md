# SkeinDB Two-Node CAS HTTP Wire Validation

Format: `skein.cas_http_wire.runtime.v1`  
Rows per scenario: **300**  
Seed: **20260922**  
Pull batch size: **32**

A transparent TCP proxy counts the exact TCP payload bytes used by the production HTTP objects.pull -> objects.fetch path. Each scenario compares a zero-overlap destination with a destination pre-seeded at the configured CAS overlap.

| Scenario | Baseline HTTP bytes | CAS HTTP bytes | HTTP bytes saved | Object bytes saved | Baseline wire / object | CAS wire / object | Second-pull HTTP bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| low_redundancy_high_churn | 143359 | 107381 | 25.1% | 25.1% | 2.54x | 2.54x | 0 |
| balanced | 76214 | 30487 | 60.0% | 60.0% | 2.55x | 2.55x | 0 |
| high_redundancy_low_churn | 16887 | 2611 | 84.5% | 86.7% | 2.55x | 2.95x | 0 |

## What the byte counter includes

- HTTP request line and request headers
- JSON-RPC request bodies containing ValueIDs
- HTTP status line and response headers
- JSON-RPC response envelopes
- Base64-encoded `bytes_b64` and `entry_b64` fields
- all other JSON syntax and metadata carried over the loopback TCP stream

## What it excludes

- Ethernet, IP, and TCP packet headers
- retransmission accounting below the TCP stream
- TLS framing, because the CI experiment uses plain loopback HTTP
- latency and throughput claims

The report therefore measures exact **HTTP-over-TCP payload bytes** seen by the transparent proxy, not packet-capture bytes on a physical network.
