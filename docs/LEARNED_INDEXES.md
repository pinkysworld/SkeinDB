# Learned Indexes for ValueID Lookup (Prototype)

Status: Experimental
Last updated: 2026-04-26

This prototype adds an in-memory ValueStore with a hybrid learned index for
ValueID lookups. The learned index is a piecewise-linear model over sorted
ValueIDs with a bounded search window. Lookups fall back to a hash map when
predictions miss, providing graceful degradation under distribution shifts.

## Components

- `ValueStore` (crates/skeindb-core): stores value bytes by ValueID and tracks
  lookup histograms.
- `LearnedIndex`: offline-built segments (slope/intercept + max error) with
  inspectable segment metadata via `ValueStore::learned_index_report()`.
- `ValueIdHistogram`: lookup distribution tracking by prefix bucket.
- Fallback index: the ordinary ValueID hash map remains live and sized in the
  learned-index report so missed predictions degrade to exact lookup.

## Refresh policy

The model rebuilds when:
- the store exceeds `min_samples` and no model exists,
- inserts since last rebuild exceed `max_inserts`,
- lookup distribution shift exceeds `max_shift_score`.

## Metrics

The ValueStore exposes:
- lookup counts, learned hit rate, average probes
- learned-index model reports with segment count, bounded search window,
  coefficient samples, and fallback entry/byte estimates
- exportable lookup histogram buckets (byte-prefix), top hot buckets, and model
  distribution shift via `ValueStore::lookup_distribution()`
- probe-count quantiles via the benchmark helper

`stats.snapshot` also exports the same runtime histogram under
`storage.value_lookup` for admin dashboards and external benchmark harnesses.

## Notes

This is a research scaffold meant to back R01 (learned indexes). It does not
change on-disk formats and can be extended to persistent models later.
