# Oblivious Execution

Status: Hardened research surface (v0.3.12)
Last updated: 2026-05-11

This document describes the R05 oblivious execution controls. The goal is to
reduce coarse access-pattern leakage in multi-tenant deployments by adding
deterministic padding, optional scan-order shuffling, and dummy ValueStore work
without changing query results.

This is not a full ORAM implementation. It is a measurable research-mode
mitigation for operators who need stable batch envelopes and evidence about the
leakage/performance trade-off.

## 1) Threat model

The R05 surface targets observers who can see coarse execution traces such as
scan batch sizes, total ValueStore lookup counts, and repeated per-table access
patterns. It does not hide query text, result sizes returned to the client,
timing side channels from arbitrary user-defined code, network metadata, or
physical storage access at an ORAM level.

Policies are scoped per table. Operators should apply them to tables where row
cardinality or predicate selectivity is sensitive, then use `oblivious.evaluate`
to compare the unpadded trace to the padded trace before enabling the policy in
production-like workloads.

## 2) Policy levels

Policies are set per table via `oblivious.policy.set`:

- `off`: no padding or dummy access.
- `basic`: scan count padding plus optional deterministic shuffle.
- `strong`: padding plus extra dummy ValueStore lookups.

## 3) Policy fields

```json
{
  "level": "basic",
  "pad_to_multiple": 32,
  "target_rows": null,
  "dummy_value_lookups": 64,
  "shuffle": false
}
```

- `pad_to_multiple`: rounds scan sizes up to the next multiple.
- `target_rows`: explicit padding target (takes precedence if larger).
- `dummy_value_lookups`: extra ValueStore lookups to obscure access counts.
- `shuffle`: optional shuffle of scan order (results are unchanged; ordering is
  only guaranteed when `ORDER BY` is used).

## 4) Runtime behavior

- Real scans return only real rows. Padding never changes the row set.
- The scan primitive computes a padded target row count from the configured
  policy and performs dummy ValueStore lookups when needed.
- Optional shuffle uses deterministic seeds based on table identity and padded
  target size, so repeated evaluation is stable.
- The limited sort/join research strategy is reported as
  `materialize_then_sort_join` for padded policies: operators should materialize
  fixed-size inputs first, then run the normal ordered operator over the padded
  envelope.

## 5) Evaluation harness

`oblivious.evaluate` returns trace-based leakage and performance evidence for a
table policy. The caller supplies optional `trace_rows`; if omitted, SkeinDB
generates boundary samples around the current row count, padding multiple, and
target row count.

Example request:

```json
{
  "table": {"db":"app","table":"users"},
  "trace_rows": [1, 2, 31, 32, 33, 63, 64, 65]
}
```

The result includes:

- `trace`: one point per sampled row count with `target_rows`, `dummy_rows`,
  `dummy_value_lookups`, `observed_accesses`, and `overhead_ratio`.
- `leakage`: empirical mutual-information-style metrics comparing actual rows
  to unpadded and padded observations, plus the number of distinct observed
  access counts.
- `performance`: mean/max overhead ratio, total dummy rows, total dummy lookups,
  and the selected padded operator strategy.

The harness is deterministic and read-only. It does not persist reports.

## 6) Admin console

SkeinAdmin exposes R05 controls in the Privacy panel. Operators can get/set the
per-table policy, explain the active padding plan, and run the leakage/overhead
evaluation from the same card. The panel sends the typed runtime payloads:
`{table:{...}, policy:{level,pad_to_multiple,target_rows,dummy_value_lookups,shuffle}}`
for `oblivious.policy.set` and `{table:{...}, trace_rows:[...]}` for
`oblivious.evaluate`.

## 7) On-disk storage

Policies are persisted to `oblivious_policies.json` (format v1). See
`docs/ON_DISK_FORMAT.md` for file placement. v0.3.12 does not change the policy
format version.
