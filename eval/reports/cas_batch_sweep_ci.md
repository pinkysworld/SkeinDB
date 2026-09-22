# SkeinDB objects.pull Batch Sweep

Format: `skein.cas_batch_sweep.runtime.v1`  
Rows per scenario: **300**  
Seed: **20260922**

The sweep varies only `objects.pull.batch_size`. CR05 compact transfer semantics, CR06 persistent keep-alive, and CR09 binary responses are held constant.

| Batch | Combined HTTP bytes | Savings vs 32 | Zero-overlap batches | CAS-overlap batches | Request bytes (zero/CAS) | Connections |
|---:|---:|---:|---:|---:|---:|---:|
| 16 | 92077 | -7.19% | 25 | 16 | 17175 / 10831 | 4 |
| 32 | 85897 | 0.00% | 13 | 8 | 15495 / 9710 | 4 |
| 64 | 82788 | 3.62% | 7 | 4 | 14643 / 9143 | 4 |
| 128 | 81548 | 5.06% | 4 | 3 | 14217 / 9001 | 4 |
| 256 | 80616 | 6.15% | 2 | 2 | 13934 / 8859 | 4 |

## Per-scenario measurements

| Scenario | Batch | Zero HTTP bytes | CAS HTTP bytes | Zero batches | CAS batches | Zero wire/materialized value | CAS wire/materialized value |
|---|---:|---:|---:|---:|---:|---:|---:|
| low_redundancy_high_churn | 16 | 36833 | 27593 | 16 | 12 | 0.654x | 0.654x |
| low_redundancy_high_churn | 32 | 34361 | 25739 | 8 | 6 | 0.610x | 0.610x |
| low_redundancy_high_churn | 64 | 33117 | 24806 | 4 | 3 | 0.588x | 0.588x |
| low_redundancy_high_churn | 128 | 32497 | 24496 | 2 | 2 | 0.577x | 0.580x |
| low_redundancy_high_churn | 256 | 32185 | 24185 | 1 | 1 | 0.571x | 0.573x |
| balanced | 16 | 19662 | 7989 | 9 | 4 | 0.659x | 0.669x |
| balanced | 32 | 18426 | 7371 | 5 | 2 | 0.618x | 0.618x |
| balanced | 64 | 17804 | 7061 | 3 | 1 | 0.597x | 0.592x |
| balanced | 128 | 17494 | 7061 | 2 | 1 | 0.586x | 0.592x |
| balanced | 256 | 17185 | 7061 | 1 | 1 | 0.576x | 0.592x |

This is a byte-efficiency sweep, not a latency or throughput benchmark. Larger batches reduce per-request HTTP/JSON framing but can increase individual response size and retry granularity.
