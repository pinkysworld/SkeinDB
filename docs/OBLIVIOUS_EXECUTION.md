# Oblivious Execution (Prototype)

Status: Draft
Last updated: 2026-01-19

This document describes the **prototype** oblivious execution controls implemented
for the R05 research agenda item. The goal is to reduce access-pattern leakage
in multi-tenant deployments by adding deterministic padding and dummy work,
without changing query results.

## 1) Policy levels

Policies are set per table via `oblivious.policy.set`:

- `off`: no padding or dummy access.
- `basic`: padding only (scan count padding, optional shuffle).
- `strong`: padding plus extra dummy ValueStore lookups.

## 2) Policy fields

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

## 3) Runtime behavior

The prototype:
- executes real scans and returns real rows unchanged.
- performs additional dummy ValueStore lookups proportional to padding.
- uses deterministic seeds based on table name for repeatable padding.

This is **not** a full ORAM implementation; it is a research scaffold intended
to exercise policy flow and accounting.

## 4) On-disk storage

Policies are persisted to `oblivious_policies.json` (format v1). See
`docs/ON_DISK_FORMAT.md` for file placement.
