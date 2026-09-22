# SkeinDB CAS Wire Composition

Format: `skein.cas_wire_composition.runtime.v1`  
Rows per scenario: **300**  
Batch size: **64**  
Seed: **20260922**

The table decomposes the exact HTTP-over-TCP payload bytes used by the compact CR05 + persistent CR06 pull path.

| Scenario | Wire bytes | Request IDs | Request envelope+HTTP | entry_b64 JSON | Response IDs | Response envelope+HTTP | Decoded transfer bytes | ValueStore object bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| low_redundancy_high_churn | 41050 | 6688 (16.3%) | 681 | 22534 (54.9%) | 6494 | 4653 | 16424 | 42211 |
| balanced | 11673 | 1891 (16.2%) | 227 | 6372 (54.6%) | 1836 | 1347 | 4644 | 11934 |

## Derived ratios

| Scenario | Base64 text / decoded transfer | Decoded transfer / ValueStore object | Total wire / ValueStore object |
|---|---:|---:|---:|
| low_redundancy_high_churn | 1.349x | 0.389x | 0.972x |
| balanced | 1.349x | 0.389x | 0.978x |

Every wire category is derived from captured HTTP bytes and the categories close exactly to the proxy total. Base64-decoded transfer-entry bytes and ValueStore object bytes are derived metrics and are not added to the wire total.

The experiment is explanatory byte accounting only; it makes no latency or throughput claim.
