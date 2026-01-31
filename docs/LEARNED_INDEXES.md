# Learned Indexes for ValueID Lookup (Prototype)

Status: Experimental
Last updated: 2026-01-18

This prototype adds an in-memory ValueStore with a hybrid learned index for
ValueID lookups. The learned index is a piecewise-linear model over sorted
ValueIDs with a bounded search window. Lookups fall back to a hash map when
predictions miss, providing graceful degradation under distribution shifts.

## Components

- `ValueStore` (crates/skeindb-core): stores value bytes by ValueID and tracks
  lookup histograms.
- `LearnedIndex`: offline-built segments (slope/intercept + max error).
- `ValueIdHistogram`: lookup distribution tracking by prefix bucket.

## Refresh policy

The model rebuilds when:
- the store exceeds `min_samples` and no model exists,
- inserts since last rebuild exceed `max_inserts`,
- lookup distribution shift exceeds `max_shift_score`.

## Metrics

The ValueStore exposes:
- lookup counts, learned hit rate, average probes
- lookup histogram buckets (byte-prefix)
- probe-count quantiles via the benchmark helper

## Notes

This is a research scaffold meant to back R01 (learned indexes). It does not
change on-disk formats and can be extended to persistent models later.
