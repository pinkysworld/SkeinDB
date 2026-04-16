#!/usr/bin/env python3
"""Synthetic evaluation harness for the compaction scheduler backlog item.

This script compares three policies over a generated workload timeline and emits:
- compaction_scheduler_summary.json
- compaction_scheduler_timeline.csv
- compaction_scheduler_dashboard.html

The model is intentionally lightweight and deterministic so it can run in CI
without external dependencies while still producing reproducible stall-rate and
p99-latency comparisons for documentation and dashboard review.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import random
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Phase:
    name: str
    seconds: int
    write_mib_s: float
    read_qps: float
    burstiness: float


@dataclass(frozen=True)
class Scenario:
    name: str
    phases: tuple[Phase, ...]


SCENARIOS = {
    "mixed": Scenario(
        name="mixed",
        phases=(
            Phase("warmup", 240, 18.0, 220.0, 0.10),
            Phase("ingest_burst", 420, 36.0, 140.0, 0.24),
            Phase("read_peak", 360, 14.0, 620.0, 0.16),
            Phase("steady_state", 420, 22.0, 340.0, 0.12),
            Phase("recovery", 360, 10.0, 180.0, 0.08),
        ),
    ),
    "write_heavy": Scenario(
        name="write_heavy",
        phases=(
            Phase("warmup", 180, 24.0, 120.0, 0.10),
            Phase("bulk_load", 540, 44.0, 80.0, 0.26),
            Phase("follow_on_writes", 480, 30.0, 110.0, 0.18),
            Phase("cooldown", 300, 12.0, 140.0, 0.08),
        ),
    ),
    "read_heavy": Scenario(
        name="read_heavy",
        phases=(
            Phase("warmup", 180, 14.0, 260.0, 0.10),
            Phase("dashboard_peak", 540, 12.0, 760.0, 0.14),
            Phase("api_peak", 420, 16.0, 680.0, 0.12),
            Phase("cooldown", 300, 10.0, 280.0, 0.08),
        ),
    ),
}


POLICY_ORDER = ["fixed_leveling", "fixed_tiering", "workload_guided"]
POLICY_COLORS = {
    "fixed_leveling": "#b45309",
    "fixed_tiering": "#0f766e",
    "workload_guided": "#1d4ed8",
}


def build_timeline(scenario: Scenario, seconds: int, rng: random.Random) -> list[dict]:
    phases = []
    total = 0
    for phase in scenario.phases:
        phase_seconds = min(phase.seconds, max(0, seconds - total))
        if phase_seconds <= 0:
            break
        phases.append(
            Phase(
                name=phase.name,
                seconds=phase_seconds,
                write_mib_s=phase.write_mib_s,
                read_qps=phase.read_qps,
                burstiness=phase.burstiness,
            )
        )
        total += phase_seconds
        if total >= seconds:
            break
    if total < seconds:
        last = phases[-1] if phases else Phase("steady_state", seconds, 20.0, 200.0, 0.08)
        phases.append(
            Phase(
                name=f"{last.name}_tail",
                seconds=seconds - total,
                write_mib_s=last.write_mib_s,
                read_qps=last.read_qps,
                burstiness=last.burstiness,
            )
        )

    timeline = []
    tick = 0
    for phase in phases:
        for _ in range(phase.seconds):
            wave = math.sin(tick / 27.0) * 0.5 + math.sin(tick / 9.0) * 0.25
            noise = rng.uniform(-phase.burstiness, phase.burstiness)
            write_mib_s = max(2.0, phase.write_mib_s * (1.0 + wave * phase.burstiness + noise))
            read_qps = max(40.0, phase.read_qps * (1.0 + wave * (phase.burstiness * 0.7) + noise * 0.45))
            timeline.append(
                {
                    "second": tick,
                    "phase": phase.name,
                    "write_mib_s": round(write_mib_s, 3),
                    "read_qps": round(read_qps, 3),
                }
            )
            tick += 1
            if tick >= seconds:
                return timeline
    return timeline


def policy_budget(policy: str, tick: dict, backlog_mib: float) -> tuple[float, float]:
    write_mib_s = tick["write_mib_s"]
    read_qps = tick["read_qps"]
    phase = tick["phase"]
    safe_mode = 0.0

    if policy == "fixed_leveling":
        budget = 20.0
        if backlog_mib > 72.0:
            budget += 12.0
        if backlog_mib > 112.0:
            budget += 10.0
            safe_mode = 1.0
        if read_qps > 650.0:
            budget *= 0.92
    elif policy == "fixed_tiering":
        budget = 14.0
        if backlog_mib > 84.0:
            budget += 6.0
        if backlog_mib > 126.0:
            budget += 8.0
            safe_mode = 1.0
        if phase.startswith("read"):
            budget *= 0.9
    else:
        backlog_ratio = backlog_mib / 96.0
        read_pressure = max(0.0, (read_qps - 360.0) / 420.0)
        write_pressure = max(0.0, (write_mib_s - 20.0) / 22.0)
        budget = 16.0 + backlog_ratio * 12.0 + write_pressure * 8.0
        if read_pressure > 0 and backlog_mib < 92.0:
            budget -= min(4.0, read_pressure * 3.0)
        if backlog_mib > 108.0:
            budget += 10.0
            safe_mode = 1.0
        if phase == "recovery":
            budget += 6.0

    return max(6.0, budget), safe_mode


def simulate_policy(policy: str, timeline: list[dict]) -> dict:
    backlog_mib = 34.0
    space_amp = 1.18
    rows = []

    for tick in timeline:
        budget_mib_s, safe_mode = policy_budget(policy, tick, backlog_mib)
        write_mib_s = tick["write_mib_s"]
        read_qps = tick["read_qps"]
        backlog_inflow = write_mib_s * 0.62
        compaction_efficiency = 0.84 if policy == "fixed_tiering" else 0.9
        compaction_done = budget_mib_s * compaction_efficiency
        backlog_mib = max(6.0, backlog_mib + backlog_inflow - compaction_done)

        pressure = backlog_mib / 96.0
        stall_ms = 0.0
        if backlog_mib > 126.0:
            stall_ms += (backlog_mib - 126.0) * 0.9
        if backlog_mib > 148.0:
            stall_ms += (backlog_mib - 148.0) * 1.6

        write_p99_ms = 5.2 + write_mib_s * 0.34 + pressure * 12.0 + budget_mib_s * 0.08 + stall_ms
        read_p99_ms = 7.1 + read_qps / 62.0 + pressure * 15.0 + budget_mib_s * 0.03

        if policy == "fixed_tiering":
            read_p99_ms += pressure * 2.3
            write_p99_ms -= 0.6
            space_amp = min(2.9, 1.25 + pressure * 0.42)
        elif policy == "fixed_leveling":
            read_p99_ms -= 0.4
            write_p99_ms += 0.5
            space_amp = min(2.4, 1.18 + pressure * 0.26)
        else:
            if safe_mode:
                write_p99_ms += 1.2
            read_p99_ms -= 0.8
            stall_ms *= 0.7
            space_amp = min(2.2, 1.14 + pressure * 0.19)

        rows.append(
            {
                "policy": policy,
                "second": tick["second"],
                "phase": tick["phase"],
                "write_mib_s": round(write_mib_s, 3),
                "read_qps": round(read_qps, 3),
                "compaction_budget_mib_s": round(budget_mib_s, 3),
                "backlog_mib": round(backlog_mib, 3),
                "stall_ms": round(stall_ms, 3),
                "write_p99_ms": round(write_p99_ms, 3),
                "read_p99_ms": round(read_p99_ms, 3),
                "space_amp": round(space_amp, 3),
                "safe_mode": int(safe_mode),
            }
        )

    return {"policy": policy, "rows": rows, "summary": summarize_policy(rows)}


def percentile(values: Iterable[float], pct: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * pct
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return float(ordered[low])
    weight = rank - low
    return float(ordered[low] * (1.0 - weight) + ordered[high] * weight)


def summarize_policy(rows: list[dict]) -> dict:
    stall_seconds = sum(1 for row in rows if row["stall_ms"] > 0)
    return {
        "samples": len(rows),
        "stall_seconds": stall_seconds,
        "stall_rate": round(stall_seconds / max(1, len(rows)), 4),
        "total_stall_ms": round(sum(row["stall_ms"] for row in rows), 2),
        "write_p99_ms_peak": round(max(row["write_p99_ms"] for row in rows), 3),
        "write_p99_ms_p95": round(percentile((row["write_p99_ms"] for row in rows), 0.95), 3),
        "read_p99_ms_peak": round(max(row["read_p99_ms"] for row in rows), 3),
        "read_p99_ms_p95": round(percentile((row["read_p99_ms"] for row in rows), 0.95), 3),
        "backlog_mib_peak": round(max(row["backlog_mib"] for row in rows), 3),
        "space_amp_avg": round(statistics.fmean(row["space_amp"] for row in rows), 3),
        "compaction_budget_mib_s_avg": round(
            statistics.fmean(row["compaction_budget_mib_s"] for row in rows), 3
        ),
    }


def svg_line_chart(series: list[tuple[str, list[tuple[float, float]]]], title: str, y_label: str) -> str:
    width = 880
    height = 250
    padding_left = 54
    padding_right = 18
    padding_top = 28
    padding_bottom = 36
    plot_w = width - padding_left - padding_right
    plot_h = height - padding_top - padding_bottom

    all_points = [point for _, points in series for point in points]
    max_x = max((point[0] for point in all_points), default=1.0)
    max_y = max((point[1] for point in all_points), default=1.0)
    min_y = min((point[1] for point in all_points), default=0.0)
    if math.isclose(max_y, min_y):
        max_y += 1.0
    y_pad = (max_y - min_y) * 0.08
    min_y -= y_pad
    max_y += y_pad

    def scale_x(x: float) -> float:
        return padding_left + (x / max(max_x, 1.0)) * plot_w

    def scale_y(y: float) -> float:
        return padding_top + (1.0 - ((y - min_y) / max(max_y - min_y, 1e-9))) * plot_h

    grid = []
    labels = []
    for idx in range(5):
        y_value = min_y + (max_y - min_y) * (idx / 4.0)
        y_pos = scale_y(y_value)
        grid.append(
            f'<line x1="{padding_left}" y1="{y_pos:.1f}" x2="{width - padding_right}" y2="{y_pos:.1f}" stroke="#dbe4f0" stroke-width="1" />'
        )
        labels.append(
            f'<text x="{padding_left - 8}" y="{y_pos + 4:.1f}" text-anchor="end" font-size="11" fill="#334155">{y_value:.1f}</text>'
        )

    x_ticks = []
    for idx in range(6):
        x_value = max_x * (idx / 5.0)
        x_pos = scale_x(x_value)
        x_ticks.append(
            f'<text x="{x_pos:.1f}" y="{height - 10}" text-anchor="middle" font-size="11" fill="#334155">{int(x_value)}</text>'
        )

    paths = []
    legend = []
    for idx, (label, points) in enumerate(series):
        color = POLICY_COLORS.get(label, "#111827")
        path_data = " ".join(
            (
                "M" if point_idx == 0 else "L"
            )
            + f" {scale_x(point[0]):.1f} {scale_y(point[1]):.1f}"
            for point_idx, point in enumerate(points)
        )
        paths.append(
            f'<path d="{path_data}" fill="none" stroke="{color}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" />'
        )
        legend_y = 18 + idx * 18
        legend.append(
            f'<rect x="{width - 200}" y="{legend_y - 8}" width="12" height="12" rx="3" fill="{color}" />'
            f'<text x="{width - 182}" y="{legend_y + 2}" font-size="12" fill="#0f172a">{html.escape(label)}</text>'
        )

    return (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">'
        + f'<text x="{padding_left}" y="18" font-size="14" font-weight="700" fill="#0f172a">{html.escape(title)}</text>'
        + f'<text x="16" y="{padding_top + plot_h / 2:.1f}" transform="rotate(-90 16 {padding_top + plot_h / 2:.1f})" font-size="11" fill="#475569">{html.escape(y_label)}</text>'
        + "".join(grid)
        + f'<line x1="{padding_left}" y1="{height - padding_bottom}" x2="{width - padding_right}" y2="{height - padding_bottom}" stroke="#94a3b8" stroke-width="1.2" />'
        + f'<line x1="{padding_left}" y1="{padding_top}" x2="{padding_left}" y2="{height - padding_bottom}" stroke="#94a3b8" stroke-width="1.2" />'
        + "".join(labels)
        + "".join(x_ticks)
        + "".join(paths)
        + "".join(legend)
        + "</svg>"
    )


def svg_bar_chart(rows: list[tuple[str, float]], title: str, y_label: str) -> str:
    width = 420
    height = 250
    padding_left = 56
    padding_right = 16
    padding_top = 28
    padding_bottom = 42
    plot_w = width - padding_left - padding_right
    plot_h = height - padding_top - padding_bottom
    max_y = max((value for _, value in rows), default=1.0)
    max_y = max(1.0, max_y * 1.15)
    bar_w = plot_w / max(1, len(rows)) * 0.62

    bars = []
    labels = []
    for idx, (label, value) in enumerate(rows):
        color = POLICY_COLORS.get(label, "#111827")
        x = padding_left + (idx + 0.2) * (plot_w / max(1, len(rows)))
        h = (value / max_y) * plot_h
        y = padding_top + (plot_h - h)
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" rx="8" fill="{color}" />'
            f'<text x="{x + bar_w / 2:.1f}" y="{y - 6:.1f}" text-anchor="middle" font-size="11" fill="#0f172a">{value:.2f}</text>'
        )
        labels.append(
            f'<text x="{x + bar_w / 2:.1f}" y="{height - 14}" text-anchor="middle" font-size="11" fill="#334155">{html.escape(label)}</text>'
        )

    return (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">'
        + f'<text x="{padding_left}" y="18" font-size="14" font-weight="700" fill="#0f172a">{html.escape(title)}</text>'
        + f'<text x="18" y="{padding_top + plot_h / 2:.1f}" transform="rotate(-90 18 {padding_top + plot_h / 2:.1f})" font-size="11" fill="#475569">{html.escape(y_label)}</text>'
        + f'<line x1="{padding_left}" y1="{height - padding_bottom}" x2="{width - padding_right}" y2="{height - padding_bottom}" stroke="#94a3b8" stroke-width="1.2" />'
        + f'<line x1="{padding_left}" y1="{padding_top}" x2="{padding_left}" y2="{height - padding_bottom}" stroke="#94a3b8" stroke-width="1.2" />'
        + "".join(bars)
        + "".join(labels)
        + "</svg>"
    )


def render_dashboard(scenario: str, seconds: int, summaries: dict, policy_rows: dict) -> str:
    stall_rows = [(policy, summaries[policy]["stall_rate"] * 100.0) for policy in POLICY_ORDER]
    write_peak_rows = [(policy, summaries[policy]["write_p99_ms_peak"]) for policy in POLICY_ORDER]
    write_series = [
        (policy, [(row["second"], row["write_p99_ms"]) for row in policy_rows[policy]])
        for policy in POLICY_ORDER
    ]
    read_series = [
        (policy, [(row["second"], row["read_p99_ms"]) for row in policy_rows[policy]])
        for policy in POLICY_ORDER
    ]
    backlog_series = [
        (policy, [(row["second"], row["backlog_mib"]) for row in policy_rows[policy]])
        for policy in POLICY_ORDER
    ]

    cards = []
    for policy in POLICY_ORDER:
        metrics = summaries[policy]
        cards.append(
            "<div class=\"card\">"
            + f"<h3>{html.escape(policy)}</h3>"
            + f"<p><strong>Stall rate</strong> {metrics['stall_rate'] * 100.0:.2f}%</p>"
            + f"<p><strong>P99 write peak</strong> {metrics['write_p99_ms_peak']:.1f} ms</p>"
            + f"<p><strong>P99 read peak</strong> {metrics['read_p99_ms_peak']:.1f} ms</p>"
            + f"<p><strong>Peak backlog</strong> {metrics['backlog_mib_peak']:.1f} MiB</p>"
            + f"<p><strong>Avg space amp</strong> {metrics['space_amp_avg']:.2f}x</p>"
            + "</div>"
        )

    table_rows = []
    for policy in POLICY_ORDER:
        metrics = summaries[policy]
        table_rows.append(
            "<tr>"
            + f"<td>{html.escape(policy)}</td>"
            + f"<td>{metrics['stall_rate'] * 100.0:.2f}%</td>"
            + f"<td>{metrics['total_stall_ms']:.1f}</td>"
            + f"<td>{metrics['write_p99_ms_p95']:.1f}</td>"
            + f"<td>{metrics['write_p99_ms_peak']:.1f}</td>"
            + f"<td>{metrics['read_p99_ms_peak']:.1f}</td>"
            + f"<td>{metrics['backlog_mib_peak']:.1f}</td>"
            + f"<td>{metrics['compaction_budget_mib_s_avg']:.1f}</td>"
            + "</tr>"
        )

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Compaction Scheduler Dashboard</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f8fafc;
      --panel: #ffffff;
      --ink: #0f172a;
      --muted: #475569;
      --line: #dbe4f0;
      --accent: #1d4ed8;
    }}
    body {{ margin: 0; font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, sans-serif; background: linear-gradient(180deg, #eff6ff 0%, var(--bg) 38%); color: var(--ink); }}
    main {{ max-width: 1200px; margin: 0 auto; padding: 32px 20px 48px; }}
    h1 {{ margin: 0 0 8px; font-size: 2rem; }}
    p.lead {{ margin: 0 0 24px; color: var(--muted); max-width: 72ch; }}
    .meta {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }}
    .chip {{ border: 1px solid var(--line); background: rgba(255,255,255,0.8); border-radius: 999px; padding: 8px 12px; font-size: 0.92rem; }}
    .cards {{ display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin-bottom: 24px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 16px 18px; box-shadow: 0 18px 42px rgba(15, 23, 42, 0.06); }}
    .charts {{ display: grid; gap: 18px; }}
    .chart {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 14px; box-shadow: 0 18px 42px rgba(15, 23, 42, 0.06); overflow-x: auto; }}
    .chart-grid {{ display: grid; gap: 18px; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); }}
    table {{ width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--line); border-radius: 18px; overflow: hidden; box-shadow: 0 18px 42px rgba(15, 23, 42, 0.06); }}
    th, td {{ padding: 12px 14px; border-bottom: 1px solid var(--line); text-align: left; font-size: 0.95rem; }}
    th {{ background: #eff6ff; }}
    tr:last-child td {{ border-bottom: none; }}
    footer {{ margin-top: 24px; color: var(--muted); font-size: 0.92rem; }}
  </style>
</head>
<body>
  <main>
    <h1>Compaction Scheduler Dashboard</h1>
    <p class=\"lead\">Synthetic evaluation harness for Phase 21 T203. The workload timeline is deterministic and highlights two operator-facing metrics: stall rate and p99 latency under fixed leveling, fixed tiering, and workload-guided scheduling.</p>
    <div class=\"meta\">
      <div class=\"chip\"><strong>Scenario</strong> {html.escape(scenario)}</div>
      <div class=\"chip\"><strong>Seconds</strong> {seconds}</div>
      <div class=\"chip\"><strong>Policies</strong> fixed_leveling, fixed_tiering, workload_guided</div>
    </div>
    <div class=\"cards\">{''.join(cards)}</div>
    <div class=\"chart-grid\">
      <div class=\"chart\">{svg_bar_chart(stall_rows, 'Stall Rate Comparison', 'stall %')}</div>
      <div class=\"chart\">{svg_bar_chart(write_peak_rows, 'P99 Write Latency Peak', 'ms')}</div>
    </div>
    <div class=\"charts\">
      <div class=\"chart\">{svg_line_chart(write_series, 'P99 Write Latency Over Time', 'ms')}</div>
      <div class=\"chart\">{svg_line_chart(read_series, 'P99 Read Latency Over Time', 'ms')}</div>
      <div class=\"chart\">{svg_line_chart(backlog_series, 'L0 Backlog Pressure', 'MiB')}</div>
    </div>
    <h2>Summary Table</h2>
    <table>
      <thead>
        <tr>
          <th>Policy</th>
          <th>Stall Rate</th>
          <th>Total Stall ms</th>
          <th>P95 of P99 Write</th>
          <th>Peak P99 Write</th>
          <th>Peak P99 Read</th>
          <th>Peak Backlog</th>
          <th>Avg Budget</th>
        </tr>
      </thead>
      <tbody>
        {''.join(table_rows)}
      </tbody>
    </table>
    <footer>Generated by eval/compaction_scheduler_dashboard.py. Use the JSON summary and CSV timeline for repeatable comparisons in papers, dashboards, or future scheduler regression tests.</footer>
  </main>
</body>
</html>
"""


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a synthetic compaction scheduler dashboard for stall rate and p99 latency."
    )
    parser.add_argument(
        "--scenario",
        choices=sorted(SCENARIOS.keys()),
        default="mixed",
        help="Synthetic workload scenario to simulate.",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=1800,
        help="Number of simulated seconds to generate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic random seed for workload shaping.",
    )
    parser.add_argument(
        "--outdir",
        default="figures/compaction_scheduler",
        help="Output directory for summary, CSV, and HTML dashboard.",
    )
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    timeline = build_timeline(SCENARIOS[args.scenario], args.seconds, random.Random(args.seed))

    runs = [simulate_policy(policy, timeline) for policy in POLICY_ORDER]
    summaries = {run["policy"]: run["summary"] for run in runs}
    rows_by_policy = {run["policy"]: run["rows"] for run in runs}
    flat_rows = [row for policy in POLICY_ORDER for row in rows_by_policy[policy]]

    summary_payload = {
        "scenario": args.scenario,
        "seconds": args.seconds,
        "seed": args.seed,
        "policies": summaries,
    }
    summary_path = outdir / "compaction_scheduler_summary.json"
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    csv_path = outdir / "compaction_scheduler_timeline.csv"
    write_csv(csv_path, flat_rows)

    dashboard_path = outdir / "compaction_scheduler_dashboard.html"
    dashboard_path.write_text(
        render_dashboard(args.scenario, args.seconds, summaries, rows_by_policy),
        encoding="utf-8",
    )

    print(f"Wrote {summary_path}")
    print(f"Wrote {csv_path}")
    print(f"Wrote {dashboard_path}")


if __name__ == "__main__":
    main()