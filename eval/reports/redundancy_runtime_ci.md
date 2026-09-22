# SkeinDB Runtime Redundancy Validation

Format: `skein.redundancy_pipeline.runtime.v1`  
Rows per scenario: **300**  
Seed: **20260922**

This report comes from a real SkeinDB process and real RPC response bodies.

| Scenario | Runtime storage saved | Runtime CAS bytes saved | Runtime QueryPatch saved | Model storage | Model CAS | Model QueryPatch |
|---|---:|---:|---:|---:|---:|---:|
| low_redundancy_high_churn | 15.0% | 25.1% | 64.6% | 4.2% | 23.3% | 82.5% |
| balanced | 55.0% | 60.0% | 84.1% | 19.0% | 57.4% | 95.2% |
| high_redundancy_low_churn | 90.0% | 86.7% | 98.2% | 54.5% | 88.3% | 98.7% |

## Evidence scope

- Storage is read directly from `stats.snapshot.storage`.
- CAS bytes are real ValueStore entry bytes counted by `objects.need` and `objects.fetch`; receiver overlap is simulated on one node, so this is not a network-throughput result.
- Query delivery counts the raw HTTP body bytes returned by `/api/v1/rpc` for `query.patch` and a full `query.select`.
- The analytical reference uses the same scenario parameters and seed.

## Interpretation

- **CAS calibration is close.** Runtime CAS savings differ from the analytical model by +1.8, +2.6, and -1.6 percentage points across the three scenarios.
- **QueryPatch has fixed protocol overhead that matters at higher churn.** The analytical model counts compact patch payloads, while the runtime measurement includes the full RPC envelope, typed literals, columns, dependencies, causality, and patch metadata. The gap shrinks from -17.9 points at high churn to -0.5 points at low churn.
- **Storage uses a different denominator by design.** Runtime `stats.snapshot.storage` reports logical ValueStore bytes versus unique ValueStore bytes, while the analytical model estimates adaptive on-disk cell-reference encoding. The runtime percentages therefore should not be treated as a direct validation of the encoding model.

The full JSON snapshot contains the byte counts and model deltas behind these percentages.
