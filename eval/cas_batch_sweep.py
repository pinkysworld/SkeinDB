#!/usr/bin/env python3
"""Sweep objects.pull batch sizes using the live two-node HTTP-wire harness.

This is an evaluation tool for CR07. It keeps the engine and transfer format
unchanged, varies only objects.pull batch_size, and measures the exact plain-HTTP
TCP payload bytes through the existing transparent proxy.

The default sweep focuses on the two scenarios with enough remote objects to
exercise multiple batches. The high-redundancy scenario has only a handful of
missing objects and therefore cannot distinguish larger batch sizes.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import redundancy_pipeline as model
import two_node_http_wire_validation as wire

FORMAT = "skein.cas_batch_sweep.runtime.v1"
DEFAULT_BATCH_SIZES = (16, 32, 64, 128, 256)


def scenario_set() -> tuple[model.Scenario, ...]:
    return (
        model.DEFAULT_SCENARIOS[0],
        model.DEFAULT_SCENARIOS[1],
    )


def summarize_run(result: dict[str, Any]) -> dict[str, Any]:
    zero = result["zero_overlap_baseline"]
    overlap = result["cas_overlap"]
    return {
        "scenario": result["scenario"],
        "batch_size": result["batch_size"],
        "source_value_ids": result["objects"]["source_value_ids"],
        "preseeded_value_ids": result["objects"]["preseeded_value_ids"],
        "zero_overlap": {
            "batches": zero["pull"]["batches"],
            "connections": zero["wire"]["connections"],
            "request_bytes": zero["wire"]["request_bytes"],
            "response_bytes": zero["wire"]["response_bytes"],
            "http_tcp_payload_bytes": zero["wire"]["http_tcp_payload_bytes"],
            "value_store_object_bytes": zero["wire"]["value_store_object_bytes"],
            "wire_object_ratio": zero["wire"]["total_wire_vs_materialized_value_ratio"],
        },
        "cas_overlap": {
            "batches": overlap["pull"]["batches"],
            "connections": overlap["wire"]["connections"],
            "request_bytes": overlap["wire"]["request_bytes"],
            "response_bytes": overlap["wire"]["response_bytes"],
            "http_tcp_payload_bytes": overlap["wire"]["http_tcp_payload_bytes"],
            "value_store_object_bytes": overlap["wire"]["value_store_object_bytes"],
            "wire_object_ratio": overlap["wire"]["total_wire_vs_materialized_value_ratio"],
        },
    }


def aggregate(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_batch: dict[int, dict[str, Any]] = {}
    for result in results:
        batch = int(result["batch_size"])
        slot = by_batch.setdefault(
            batch,
            {
                "batch_size": batch,
                "zero_overlap_http_bytes": 0,
                "cas_overlap_http_bytes": 0,
                "zero_overlap_request_bytes": 0,
                "cas_overlap_request_bytes": 0,
                "zero_overlap_batches": 0,
                "cas_overlap_batches": 0,
                "connections": 0,
            },
        )
        zero = result["zero_overlap"]
        cas = result["cas_overlap"]
        slot["zero_overlap_http_bytes"] += int(zero["http_tcp_payload_bytes"])
        slot["cas_overlap_http_bytes"] += int(cas["http_tcp_payload_bytes"])
        slot["zero_overlap_request_bytes"] += int(zero["request_bytes"])
        slot["cas_overlap_request_bytes"] += int(cas["request_bytes"])
        slot["zero_overlap_batches"] += int(zero["batches"])
        slot["cas_overlap_batches"] += int(cas["batches"])
        slot["connections"] += int(zero["connections"]) + int(cas["connections"])

    ordered = [by_batch[key] for key in sorted(by_batch)]
    if not ordered:
        return ordered

    baseline = next(
        (row for row in ordered if row["batch_size"] == 32),
        ordered[0],
    )
    base_total = (
        baseline["zero_overlap_http_bytes"] + baseline["cas_overlap_http_bytes"]
    )
    for row in ordered:
        total = row["zero_overlap_http_bytes"] + row["cas_overlap_http_bytes"]
        row["combined_http_bytes"] = total
        row["combined_http_bytes_vs_32"] = total - base_total
        row["combined_savings_vs_32_pct"] = (
            round(100.0 * (base_total - total) / base_total, 4)
            if base_total
            else 0.0
        )
    return ordered


def validate(results: list[dict[str, Any]], aggregate_rows: list[dict[str, Any]]) -> None:
    if not results:
        raise RuntimeError("batch sweep produced no results")

    for result in results:
        zero = result["zero_overlap"]
        cas = result["cas_overlap"]
        if zero["connections"] != 1 or cas["connections"] != 1:
            raise RuntimeError(
                f"{result['scenario']} batch={result['batch_size']}: "
                "CR06 keep-alive regression detected"
            )
        if zero["http_tcp_payload_bytes"] <= 0 or cas["http_tcp_payload_bytes"] <= 0:
            raise RuntimeError("batch sweep measured non-positive wire bytes")

    totals = [row["combined_http_bytes"] for row in aggregate_rows]
    if totals != sorted(totals, reverse=True):
        raise RuntimeError(
            "larger batch sizes did not monotonically reduce combined HTTP bytes: "
            + repr(totals)
        )


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# SkeinDB objects.pull Batch Sweep",
        "",
        f"Format: `{report['format']}`  ",
        f"Rows per scenario: **{report['rows']}**  ",
        f"Seed: **{report['seed']}**",
        "",
        "The sweep varies only `objects.pull.batch_size`. CR05 compact transfer semantics, CR06 persistent keep-alive, and CR09 binary responses are held constant.",
        "",
        "| Batch | Combined HTTP bytes | Savings vs 32 | Zero-overlap batches | CAS-overlap batches | Request bytes (zero/CAS) | Connections |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report["aggregate"]:
        lines.append(
            "| {batch} | {total} | {saving:.2f}% | {zb} | {cb} | {zr} / {cr} | {conn} |".format(
                batch=row["batch_size"],
                total=row["combined_http_bytes"],
                saving=row["combined_savings_vs_32_pct"],
                zb=row["zero_overlap_batches"],
                cb=row["cas_overlap_batches"],
                zr=row["zero_overlap_request_bytes"],
                cr=row["cas_overlap_request_bytes"],
                conn=row["connections"],
            )
        )

    lines.extend(
        [
            "",
            "## Per-scenario measurements",
            "",
            "| Scenario | Batch | Zero HTTP bytes | CAS HTTP bytes | Zero batches | CAS batches | Zero wire/materialized value | CAS wire/materialized value |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in report["results"]:
        lines.append(
            "| {scenario} | {batch} | {zero} | {cas} | {zb} | {cb} | {zr:.3f}x | {cr:.3f}x |".format(
                scenario=row["scenario"],
                batch=row["batch_size"],
                zero=row["zero_overlap"]["http_tcp_payload_bytes"],
                cas=row["cas_overlap"]["http_tcp_payload_bytes"],
                zb=row["zero_overlap"]["batches"],
                cb=row["cas_overlap"]["batches"],
                zr=row["zero_overlap"]["wire_object_ratio"],
                cr=row["cas_overlap"]["wire_object_ratio"],
            )
        )
    lines.extend(
        [
            "",
            "This is a byte-efficiency sweep, not a latency or throughput benchmark. Larger batches reduce per-request HTTP/JSON framing but can increase individual response size and retry granularity.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skeindb-bin", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=300)
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument(
        "--batch-sizes",
        default="16,32,64,128,256",
        help="comma-separated batch sizes",
    )
    parser.add_argument("--base-port", type=int, default=19100)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binary = args.skeindb_bin.resolve()
    if not binary.exists():
        raise SystemExit(f"SkeinDB binary does not exist: {binary}")

    batch_sizes = tuple(
        sorted({int(part.strip()) for part in args.batch_sizes.split(",") if part.strip()})
    )
    if not batch_sizes or any(size <= 0 for size in batch_sizes):
        raise SystemExit("batch sizes must be positive")

    results: list[dict[str, Any]] = []
    run_index = 0
    for scenario_index, scenario in enumerate(scenario_set()):
        for batch_size in batch_sizes:
            base = args.base_port + run_index * 20
            full = wire.run_scenario(
                binary,
                scenario,
                args.rows,
                args.seed + scenario_index * 101,
                batch_size,
                source_http_port=base,
                baseline_http_port=base + 1,
                overlap_http_port=base + 2,
                proxy_port=base + 3,
                source_cluster_port=base + 100,
                baseline_cluster_port=base + 101,
                overlap_cluster_port=base + 102,
                keep_logs=None,
            )
            summary = summarize_run(full)
            results.append(summary)
            print(
                "batch sweep {scenario} batch={batch}: zero={zero}B/{zb} batches "
                "cas={cas}B/{cb} batches".format(
                    scenario=summary["scenario"],
                    batch=batch_size,
                    zero=summary["zero_overlap"]["http_tcp_payload_bytes"],
                    zb=summary["zero_overlap"]["batches"],
                    cas=summary["cas_overlap"]["http_tcp_payload_bytes"],
                    cb=summary["cas_overlap"]["batches"],
                ),
                flush=True,
            )
            run_index += 1

    aggregate_rows = aggregate(results)
    validate(results, aggregate_rows)
    report = {
        "format": FORMAT,
        "kind": "live_objects_pull_batch_size_sweep",
        "rows": args.rows,
        "seed": args.seed,
        "batch_sizes": list(batch_sizes),
        "claim_boundary": [
            "Exact plain-HTTP TCP payload bytes are measured with the same transparent proxy as the CAS wire benchmark.",
            "CR05 transfer_only encoding and CR06 persistent-client behavior are held constant.",
            "No latency or throughput conclusion is drawn from CI wall-clock time.",
        ],
        "aggregate": aggregate_rows,
        "results": results,
    }

    json_text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    markdown = render_markdown(report)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json_text, encoding="utf-8")
    if args.markdown_out:
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(markdown, encoding="utf-8")
    if not args.json_out and not args.markdown_out:
        print(json_text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
