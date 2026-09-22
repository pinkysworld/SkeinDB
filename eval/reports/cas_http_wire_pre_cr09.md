# SkeinDB Two-Node CAS HTTP Wire Validation

Format: `skein.cas_http_wire.runtime.v1`  
Rows per scenario: **300**  
Seed: **20260922**  
Pull batch size: **32**

A transparent TCP proxy counts the exact TCP payload bytes used by the production HTTP objects.pull -> objects.fetch path. Each scenario compares a zero-overlap destination with a destination pre-seeded at the configured CAS overlap.

| Scenario | Baseline HTTP bytes | CAS HTTP bytes | HTTP bytes saved | Object bytes saved | Baseline connections | CAS connections | Baseline wire / object | CAS wire / object | Second-pull HTTP bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| low_redundancy_high_churn | 56564 | 42370 | 25.1% | 25.1% | 1 | 1 | 1.00x | 1.00x | 0 |
| balanced | 30280 | 12113 | 60.0% | 60.0% | 1 | 1 | 1.01x | 1.01x | 0 |
| high_redundancy_low_churn | 6677 | 1267 | 81.0% | 86.7% | 1 | 1 | 1.01x | 1.43x | 0 |

## What the byte counter includes

- HTTP request line and request headers
- JSON-RPC request bodies containing ValueIDs
- HTTP status line and response headers
- JSON-RPC response envelopes
- Base64-encoded `entry_b64` transfer payloads used by `objects.pull`
- all other JSON syntax and metadata carried over the loopback TCP stream
- the legacy `bytes_b64` field is compatibility-tested separately and is not present in the measured CR05 pull traffic

## What it excludes

- Ethernet, IP, and TCP packet headers
- retransmission accounting below the TCP stream
- TLS framing, because the CI experiment uses plain loopback HTTP
- latency and throughput claims

The report therefore measures exact **HTTP-over-TCP payload bytes** seen by the transparent proxy, not packet-capture bytes on a physical network.
