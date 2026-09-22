# SkeinDB objects.pull Batch Sweep

Format: `skein.cas_batch_sweep.runtime.v1`  
Rows per scenario: **300**  
Seed: **20260922**

The sweep varies only `objects.pull.batch_size`. CR05 compact transfer encoding and CR06 persistent keep-alive are held constant.

| Batch | Combined HTTP bytes | Savings vs 32 | Zero-overlap batches | CAS-overlap batches | Request bytes (zero/CAS) | Connections |
|---:|---:|---:|---:|---:|---:|---:|
| 16 | 150108 | -6.21% | 25 | 16 | 19325 / 12207 | 4 |
| 32 | 141327 | 0.00% | 13 | 8 | 16613 / 10398 | 4 |
| 64 | 136927 | 3.11% | 7 | 4 | 15245 / 9487 | 4 |
| 128 | 135159 | 4.36% | 4 | 3 | 14561 / 9259 | 4 |
| 256 | 133835 | 5.30% | 2 | 2 | 14106 / 9031 | 4 |

## Per-scenario measurements

| Scenario | Batch | Zero HTTP bytes | CAS HTTP bytes | Zero batches | CAS batches | Zero wire/object | CAS wire/object |
|---|---:|---:|---:|---:|---:|---:|---:|
| low_redundancy_high_churn | 16 | 60076 | 45004 | 16 | 12 | 1.066x | 1.066x |
| low_redundancy_high_churn | 32 | 56564 | 42370 | 8 | 6 | 1.004x | 1.004x |
| low_redundancy_high_churn | 64 | 54804 | 41050 | 4 | 3 | 0.972x | 0.972x |
| low_redundancy_high_churn | 128 | 53920 | 40608 | 2 | 2 | 0.957x | 0.962x |
| low_redundancy_high_churn | 256 | 53478 | 40166 | 1 | 1 | 0.949x | 0.952x |
| balanced | 16 | 32036 | 12992 | 9 | 4 | 1.074x | 1.089x |
| balanced | 32 | 30280 | 12113 | 5 | 2 | 1.015x | 1.015x |
| balanced | 64 | 29400 | 11673 | 3 | 1 | 0.985x | 0.978x |
| balanced | 128 | 28958 | 11673 | 2 | 1 | 0.971x | 0.978x |
| balanced | 256 | 28518 | 11673 | 1 | 1 | 0.956x | 0.978x |

This is a byte-efficiency sweep, not a latency or throughput benchmark. Larger batches reduce per-request HTTP/JSON-RPC framing but can increase individual response size and retry granularity.
