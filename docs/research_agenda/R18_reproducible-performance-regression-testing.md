# R18 — Reproducible Performance Regression Testing

**Area:** Developer Experience & Tooling

## Problem Statement

SkeinDB's replay bundles enable reproducing correctness bugs, but performance issues are often harder to reproduce. Performance depends on state that isn't captured in traditional replay: compaction state, cache contents, concurrent load. Extending replay bundles to capture performance-relevant state could enable reproducible performance regression testing, critical for database development and deployment confidence.

## Research Hypotheses

- **H1:** Performance-critical state (compaction level, cache hotness, concurrent operations) can be captured alongside data state in extended replay bundles.
- **H2:** Deterministic replay of performance-annotated bundles reproduces performance characteristics within acceptable variance.
- **H3:** Performance replay enables root cause analysis for production performance regressions.

## Methodology

- Phase 1 - State Identification: Identify performance-critical state: (a) LSM level structure, (b) block cache contents, (c) connection pool state, (d) compaction queue. Determine minimal state for reproducibility.
- Phase 2 - Bundle Extension: Extend replay bundle format to include: (a) LSM metadata snapshot, (b) cache state approximation (LRU ordering, hot keys), (c) timing annotations on WAL records.
- Phase 3 - Deterministic Replay: Implement replay mode that: (a) reconstructs storage state to bundle snapshot, (b) warms cache according to bundle, (c) replays operations with timing annotations.
- Phase 4 - Regression Detection: Build regression testing framework: (a) capture bundles from production, (b) replay in test environment, (c) compare latency distributions, (d) alert on significant deviations.

## Evaluation Plan

- **E1:** Reproducibility: variance in replayed performance vs. original.
- **E2:** Bundle size overhead for performance state vs. data-only bundles.
- **E3:** Replay fidelity: do cache warmup and LSM reconstruction match original?
- **E4:** Regression detection: can the framework catch known performance bugs?
- **E5:** Developer workflow: time to diagnose performance issue with vs. without replay.

## Expected Contributions

- Extended replay bundle format for performance reproducibility.
- Deterministic replay framework for database performance testing.
- Methodology for capturing and reconstructing performance-critical state.
- Performance regression testing framework for database development.

## Key Related Work

- Curtsinger & Berger 'STABILIZER' (2013); Tene et al. 'jHiccup' (2013)

## Integration into SkeinDB

This section is an *adaptation* of the research direction into SkeinDB’s architecture and backlog.

- **Primary building blocks used:** ValueID store, SkeinQL, dependency tracking, hash-chained WAL, Wasm runtime, LSM/compaction.
- **Spec touchpoints:** add or extend a doc under `docs/` and add corresponding SkeinQL methods under `docs/SKEINQL.md` (experimental).
- **Backlog hook:** see `docs/RESEARCH_BACKLOG.md` for tasks mapped to this proposal.

Current adaptation:

- Replay bundles now carry optional `skein.replay.performance.v1` metadata with storage/LSM counters, cache warm hints, and timing summaries.
- `maintenance.replay.run` emits a variance report for annotated bundles, rehydrates captured select/patch cache counts in the replay workspace, and compares a normalized replay-run checksum over reconstructable snapshot state.
- Snapshot bundles still rely on retained change-event metadata plus table snapshots rather than impossible row-by-row WAL mutation replay.
- The raw `disk_bytes` / `wal_bytes` fields remain part of the variance report instead of the replay-run checksum so workspace-local artifact files do not cause false mismatches.
