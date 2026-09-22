# SkeinDB CAS Wire Composition

Format: `skein.cas_wire_composition.runtime.v1`  
Rows per scenario: **300**  
Batch size: **64**  
Seed: **20260922**

The table decomposes the exact HTTP-over-TCP payload bytes used by the CR09 binary-response pull path. The request remains JSON; the response carries canonical transfer entries directly.

| Scenario | Wire bytes | Request IDs | Request envelope+HTTP | Binary transfer entries | Binary framing | Response envelope+HTTP | Decoded transfer bytes | ValueStore object bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| low_redundancy_high_churn | 24806 | 6688 (27.0%) | 423 | 16424 (66.2%) | 791 | 480 | 16424 | 42211 |
| balanced | 7061 | 1891 (26.8%) | 141 | 4644 (65.8%) | 225 | 160 | 4644 | 11934 |

## Derived ratios

| Scenario | Response mode | Decoded transfer / ValueStore object | Total wire / ValueStore object |
|---|---|---:|---:|
| low_redundancy_high_churn | binary_v1 | 0.389x | 0.588x |
| balanced | binary_v1 | 0.389x | 0.592x |

Every wire category is derived from captured HTTP bytes and the categories close exactly to the proxy total. Canonical transfer-entry bytes and ValueStore object bytes are derived metrics and are not double-counted.

The experiment is explanatory byte accounting only; it makes no latency or throughput claim.
