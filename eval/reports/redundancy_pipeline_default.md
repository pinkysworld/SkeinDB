# SkeinDB Redundancy Pipeline Evaluation

Format: `skein.redundancy_pipeline.v1`  
Rows per scenario: **600**  
Seed: **20260922**

This deterministic harness uses one workload to account for redundant bytes at three layers: storage, CAS replication, and query delivery.

| Scenario | Storage saved | CAS object bytes saved | QueryPatch saved | Lifecycle byte-work saved | Delivery |
|---|---:|---:|---:|---:|---|
| low_redundancy_high_churn | 4.2% | 23.5% | 82.5% | 39.5% | patch |
| balanced | 19.0% | 60.6% | 95.5% | 60.1% | patch |
| high_redundancy_low_churn | 54.5% | 89.5% | 99.0% | 79.0% | patch |

## Scope and limitations

- Storage numbers cover serialized cell values, not complete row/file framing.
- CAS replication applies the documented U-I object-byte bound and excludes row metadata/protocol overhead.
- Query delivery uses compact JSON without HTTP compression and models add/update/remove patches.
- Lifecycle byte-work is a cross-layer accounting metric, not bytes on one physical channel.

The JSON report contains the byte counts behind every percentage.
