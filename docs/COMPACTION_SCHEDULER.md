# Workload-guided compaction scheduling

Status: Draft + live telemetry + runtime policy controls + evaluation harness
Last updated: 2026-04-16

SkeinDB uses an LSM-like structure for sorted runs and background compaction.
Compaction is necessary to bound read amplification and reclaim space, but can also cause
write stalls and tail-latency spikes under load.

This document specifies a workload-guided compaction scheduler that:
- reduces stall probability and p99 latency for OLTP workloads,
- adapts to changing read/write mixes,
- maintains explicit bounds on space amplification.

## 1. Background

LSM-tree based systems trade write throughput for background merging work.
Compaction policy and scheduling strongly affect read amplification, write amplification, and performance stability.

In practice, engines often use fixed policies chosen by humans. Research and production experience both
show that workload-aware approaches can improve throughput and reduce stalls.

## 2. Goals and non-goals

Goals:
- Avoid user-visible stalls whenever possible.
- Smooth compaction work (avoid "sawtooth" behavior).
- Provide a predictable space bound (cap L0 and overall level sizes).
- Be controllable: administrators can set budgets and windows.

Non-goals (v1):
- Perfect optimality. v1 uses heuristics; later versions may use cost models.

## 3. Inputs (telemetry)

The scheduler relies on telemetry already collected for SkeinAdmin:

- write rate (bytes/s, ops/s)
- read rate (point/range)
- p50/p95/p99 read and write latency
- L0 file count / bytes
- per-level bytes, pending merges
- stall events and reasons
- snapshot build activity (if enabled)

Telemetry should be stored in a privacy-safe, bounded ring buffer.

Current baseline:
- `stats.snapshot` now exports a top-level `compaction` object with live `status`, `l0_files`, `l0_bytes`, `l0_pressure_pct`, `stall_rate`, `pressure_rate`, bounded pressure-event history, recent point/range/write rates, and read/write p50/p95/p99 latency percentiles.
- L0 pressure is derived from live `.rseg` files under `tables/<db>/*.rseg`, and recent workload signals are derived from the existing bounded RPC/query telemetry ring buffer.
- `stats.snapshot.compaction.scheduler` now reports the live policy, configured/effective budgets, active peak window, queued task summary, and priority/admission state derived from the same runtime telemetry.
- `maintenance.compaction.status`, `maintenance.compaction.set_policy`, and `maintenance.compaction.pause` / `resume` persist policy state in `settings.json`, expose live scheduler decisions, and enforce safe-mode write backpressure for write-classified SkeinQL/HTTP mutations when hard L0 bounds are exceeded.

## 4. Compaction task model

Each compaction candidate is modeled as:

- kind: L0->L1, Li->Li+1, tombstone cleanup, delta rebase, snapshot cseg rebuild
- estimated bytes read/write
- estimated overlap and CPU
- expected benefit:
  - reduce read amplification
  - reclaim space
  - reduce stall risk (L0 pressure)

## 5. Scheduling policy (v1 heuristic)

### 5.1 Budgets

- max_compaction_io_bytes_per_s
- max_compaction_cpu_pct
- peak_hours windows (optional): reduce budgets or pause non-urgent compactions

### 5.2 Priority score

Compute a priority score per task:

priority = w_stall * stall_risk + w_space * space_pressure + w_read * read_pressure + w_health * (age)

Where:
- stall_risk increases rapidly when L0 file count or bytes approach configured limits
- space_pressure increases when total bytes exceed target
- read_pressure increases when read amplification is high and read latency is above target
- age prevents starvation (task age increases over time)

### 5.3 Partial compactions

Prefer partial compactions (small file sets) to amortize cost and avoid long spikes.

### 5.4 Safe mode

If the system approaches hard limits (e.g., L0 too large), the scheduler enters safe mode:
- compaction budgets are temporarily increased
- write admission may be throttled (backpressure)
- read-only traffic remains available while write-classified SkeinQL/HTTP requests are rejected with `resource_exhausted`

## 6. Administrator controls

Expose in settings:
- compaction.enabled
- compaction.budget
- compaction.peak_windows
- compaction.max_l0_files, compaction.max_l0_bytes

## 7. SkeinQL surface

Methods:
- maintenance.compaction.status
- maintenance.compaction.set_policy
- maintenance.compaction.pause / resume

Stats:
- `stats.snapshot` includes live compaction pressure/workload telemetry plus `compaction.scheduler` state for budgets, task priority, peak-window activation, and write-admission mode.

## 8. Evaluation plan

Measure:
- p99 write latency and stall frequency under YCSB-like workloads
- throughput under dynamic read/write mixes
- space amplification and write amplification

Compare:
- fixed leveling policy
- fixed tiering policy
- workload-guided scheduler

Prototype harness:
- `python3 eval/compaction_scheduler_dashboard.py --scenario mixed --seconds 1800 --outdir eval/figures/compaction_scheduler`
- emits `compaction_scheduler_summary.json`, `compaction_scheduler_timeline.csv`, and a self-contained `compaction_scheduler_dashboard.html`
- compares `fixed_leveling`, `fixed_tiering`, and `workload_guided` policies over the same deterministic workload timeline
- surfaces stall rate, total stall time, peak/p95-of-p99 read and write latency, backlog pressure, and average compaction budget

The harness remains synthetic rather than tied directly to a background compactor. That keeps the evaluation slice reproducible while the runtime policy layer uses the same pressure/workload inputs to drive budget selection, peak-window scaling, and safe-mode admission control.

## 9. Testing

- scheduler never violates hard bounds without applying backpressure
- no starvation: tasks make progress over time
- policy changes take effect without restart

---

## Research extension: Energy-aware compaction scheduling

This spec focuses on workload-guided scheduling to reduce stalls and tail latency.
The research agenda proposes an additional objective: **energy minimization** (or carbon/cost minimization) under performance constraints.
See: `docs/research_agenda/R20_energy-aware-compaction-scheduling.md`.

Adaptation points:
- Add an energy cost model for compaction work (CPU + IO estimation; optional external signals).
- Prototype instrumentation: delta compaction reports `energy` (cpu_units, io_bytes_read, io_bytes_written, energy_score) to seed scheduling heuristics.
- Treat policy as constrained optimization: minimize energy subject to read amplification, write amplification, and backlog bounds.
- Accept external signals where available (battery vs plugged, time-of-use price windows, carbon intensity), but keep the baseline functional without them.
