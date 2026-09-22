#!/usr/bin/env python3
"""Two-node CAS transfer validation for SkeinDB.

This harness starts two independent SkeinDB processes:

- source: owns the complete ValueStore object set
- destination: is pre-seeded with a deterministic fraction of the same ValueIDs

The destination then calls objects.pull against the source over real HTTP. The
report records the pre-existing object bytes on the destination, object bytes
actually served by the source, pull/import verification, and an idempotence
check proving that a second pull performs no additional remote fetches.

This is a real two-process transfer experiment. The byte counters are ValueStore
entry bytes rather than packet captures, so no transport framing/compression
claim is made.
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import redundancy_pipeline as model
import runtime_redundancy_validation as runtime

FORMAT = "skein.cas_two_node.runtime.v1"


def create_table(base_url: str, db: str, table: str, request_id: int) -> int:
    runtime.rpc(base_url, "schema.create_database", {"db": db}, request_id)
    request_id += 1
    runtime.rpc(
        base_url,
        "schema.create_table",
        {
            "db": db,
            "table": table,
            "primary_key": ["id"],
            "columns": [
                {"name": "id", "type": {"kind": "i64"}, "nullable": False},
                {"name": "payload", "type": {"kind": "string"}, "nullable": False},
                {"name": "revision", "type": {"kind": "i64"}, "nullable": False},
            ],
        },
        request_id,
    )
    return request_id + 1


def insert_literal_rows(
    base_url: str,
    db: str,
    table: str,
    rows: list[dict[str, Any]],
    request_id: int,
) -> int:
    for start in range(0, len(rows), 100):
        batch = rows[start : start + 100]
        runtime.rpc(
            base_url,
            "data.insert",
            {"into": {"db": db, "table": table}, "rows": batch},
            request_id,
        )
        request_id += 1
    return request_id


def prefill_rows(
    value_ids: list[str], dictionary: dict[str, Any]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, value_id in enumerate(value_ids, start=1):
        literal = dictionary[value_id]
        rows.append(
            {
                "id": {"t": "i64", "v": idx},
                "payload": literal,
                "revision": {"t": "i64", "v": 0},
            }
        )
    return rows


def delta(after: dict[str, Any], before: dict[str, Any], key: str) -> int:
    return int(after.get(key) or 0) - int(before.get(key) or 0)


def pct(saved: int | float, baseline: int | float) -> float:
    return round(100.0 * saved / baseline, 3) if baseline else 0.0


def launch_node(
    skeindb_bin: Path,
    data_dir: Path,
    http_port: int,
    cluster_port: int,
    log_path: Path,
) -> tuple[subprocess.Popen[str], Any, str]:
    log_handle = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [
            str(skeindb_bin),
            "serve",
            "--data",
            str(data_dir),
            "--http",
            str(http_port),
            "--mysql",
            "0",
            "--pg",
            "0",
            "--cluster-port",
            str(cluster_port),
        ],
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url = f"http://127.0.0.1:{http_port}"
    runtime.wait_ready(base_url, proc)
    return proc, log_handle, base_url


def stop_node(proc: subprocess.Popen[str], log_handle: Any, base_url: str, request_id: int) -> None:
    try:
        if proc.poll() is None:
            runtime.rpc(base_url, "system.shutdown", {}, request_id)
            proc.wait(timeout=5)
    except Exception:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
    finally:
        log_handle.close()


def validate_scenario_result(result: dict[str, Any]) -> None:
    object_count = result["objects"]["source_value_ids"]
    preseeded = result["objects"]["preseeded_value_ids"]
    missing = result["objects"]["preflight_missing_ids"]
    pull = result["first_pull"]
    second = result["second_pull"]

    if preseeded + missing != object_count:
        raise RuntimeError(
            f"{result['scenario']}: preseeded + missing != source object count"
        )
    if pull["already_present"] != preseeded:
        raise RuntimeError(
            f"{result['scenario']}: objects.pull already_present mismatch"
        )
    if pull["fetched_objects"] != missing or pull["stored"] != missing:
        raise RuntimeError(
            f"{result['scenario']}: first pull did not fetch/store every missing object"
        )
    if pull["remote_missing"] or pull["verification_failed"] or pull["invalid_ids"]:
        raise RuntimeError(
            f"{result['scenario']}: first pull reported transfer/integrity failures"
        )
    if result["postflight_missing_ids"] != 0:
        raise RuntimeError(
            f"{result['scenario']}: destination still misses objects after pull"
        )
    if result["source_fetch"]["objects_served"] != missing:
        raise RuntimeError(
            f"{result['scenario']}: source objects.fetch count does not match preflight missing set"
        )
    if result["source_fetch"]["obj_bytes"] <= 0:
        raise RuntimeError(f"{result['scenario']}: source transferred zero object bytes")
    if second["already_present"] != object_count:
        raise RuntimeError(
            f"{result['scenario']}: second pull did not see every object locally"
        )
    if second["fetched_objects"] != 0 or second["stored"] != 0 or second["batches"] != 0:
        raise RuntimeError(
            f"{result['scenario']}: second pull was not locally idempotent"
        )
    if result["second_pull_source_fetch"]["fetch_calls"] != 0:
        raise RuntimeError(
            f"{result['scenario']}: second pull made an unexpected remote fetch call"
        )
    if result["second_pull_source_fetch"]["objects_served"] != 0:
        raise RuntimeError(
            f"{result['scenario']}: second pull served unexpected remote objects"
        )
    if result["second_pull_source_fetch"]["obj_bytes"] != 0:
        raise RuntimeError(
            f"{result['scenario']}: second pull transferred unexpected object bytes"
        )


def run_scenario(
    skeindb_bin: Path,
    scenario: model.Scenario,
    rows_count: int,
    seed: int,
    batch_size: int,
    source_http_port: int,
    dest_http_port: int,
    source_cluster_port: int,
    dest_cluster_port: int,
    keep_logs: Path | None,
) -> dict[str, Any]:
    source_rows = model.build_rows(rows_count, scenario.repetition, seed)
    analytical = model.run_scenario(scenario, rows_count, seed)

    with tempfile.TemporaryDirectory(prefix="skeindb-cas-two-node-") as tmp:
        root = Path(tmp)
        source_log = root / "source.log"
        dest_log = root / "destination.log"
        source_proc, source_handle, source_url = launch_node(
            skeindb_bin,
            root / "source-data",
            source_http_port,
            source_cluster_port,
            source_log,
        )
        dest_proc, dest_handle, dest_url = launch_node(
            skeindb_bin,
            root / "destination-data",
            dest_http_port,
            dest_cluster_port,
            dest_log,
        )
        source_request = 100
        dest_request = 10_000

        try:
            source_request = create_table(
                source_url, "cas_source", "items", source_request
            )
            source_request = runtime.insert_rows(
                source_url,
                "cas_source",
                "items",
                source_rows,
                source_request,
            )

            query = runtime.query_ast("cas_source", "items")
            skeinpack, _ = runtime.rpc(
                source_url,
                "query.select",
                {
                    "query": query,
                    "args": [],
                    "result_format": "skeinpack_v1",
                    "cache": {"want_etag": True},
                    "wire": {"format": "skeinpack_v1", "known_valueids": []},
                },
                source_request,
            )
            source_request += 1
            dictionary = skeinpack["result"].get("data", {}).get("dict", {})
            value_ids = sorted(
                value_id
                for value_id in dictionary
                if isinstance(value_id, str) and len(value_id) == 32
            )
            if len(value_ids) < 2:
                raise RuntimeError(
                    f"{scenario.name}: skeinpack exposed too few source ValueIDs"
                )

            target_preseed = int(round(len(value_ids) * scenario.receiver_overlap))
            target_preseed = max(1, min(len(value_ids) - 1, target_preseed))
            preseed_ids = value_ids[:target_preseed]

            dest_request = create_table(
                dest_url, "cas_destination", "seed_values", dest_request
            )
            dest_request = insert_literal_rows(
                dest_url,
                "cas_destination",
                "seed_values",
                prefill_rows(preseed_ids, dictionary),
                dest_request,
            )

            dest_stats_before, _ = runtime.rpc(
                dest_url, "cluster.replication_stats", {}, dest_request
            )
            dest_request += 1
            preflight, _ = runtime.rpc(
                dest_url, "objects.need", {"ids": value_ids}, dest_request
            )
            dest_request += 1
            dest_stats_after, _ = runtime.rpc(
                dest_url, "cluster.replication_stats", {}, dest_request
            )
            dest_request += 1

            preflight_present = preflight["result"].get("present", [])
            preflight_missing = preflight["result"].get("missing", [])
            if len(preflight_present) != target_preseed:
                raise RuntimeError(
                    f"{scenario.name}: destination preseed exposed "
                    f"{len(preflight_present)} ValueIDs, expected {target_preseed}"
                )

            dest_ref_bytes = delta(
                dest_stats_after["result"], dest_stats_before["result"], "ref_bytes"
            )

            source_stats_before, _ = runtime.rpc(
                source_url, "cluster.replication_stats", {}, source_request
            )
            source_request += 1

            first_pull, _ = runtime.rpc(
                dest_url,
                "objects.pull",
                {
                    "source_rpc_url": source_url,
                    "ids": value_ids,
                    "batch_size": batch_size,
                },
                dest_request,
            )
            dest_request += 1

            source_stats_after, _ = runtime.rpc(
                source_url, "cluster.replication_stats", {}, source_request
            )
            source_request += 1
            first_source_delta = {
                "fetch_calls": delta(
                    source_stats_after["result"],
                    source_stats_before["result"],
                    "fetch_calls",
                ),
                "fetch_ids_total": delta(
                    source_stats_after["result"],
                    source_stats_before["result"],
                    "fetch_ids_total",
                ),
                "objects_served": delta(
                    source_stats_after["result"],
                    source_stats_before["result"],
                    "fetch_objects_served",
                ),
                "obj_bytes": delta(
                    source_stats_after["result"],
                    source_stats_before["result"],
                    "obj_bytes",
                ),
            }

            postflight, _ = runtime.rpc(
                dest_url, "objects.need", {"ids": value_ids}, dest_request
            )
            dest_request += 1

            source_second_before, _ = runtime.rpc(
                source_url, "cluster.replication_stats", {}, source_request
            )
            source_request += 1
            second_pull, _ = runtime.rpc(
                dest_url,
                "objects.pull",
                {
                    "source_rpc_url": source_url,
                    "ids": value_ids,
                    "batch_size": batch_size,
                },
                dest_request,
            )
            dest_request += 1
            source_second_after, _ = runtime.rpc(
                source_url, "cluster.replication_stats", {}, source_request
            )
            source_request += 1

            second_source_delta = {
                "fetch_calls": delta(
                    source_second_after["result"],
                    source_second_before["result"],
                    "fetch_calls",
                ),
                "fetch_ids_total": delta(
                    source_second_after["result"],
                    source_second_before["result"],
                    "fetch_ids_total",
                ),
                "objects_served": delta(
                    source_second_after["result"],
                    source_second_before["result"],
                    "fetch_objects_served",
                ),
                "obj_bytes": delta(
                    source_second_after["result"],
                    source_second_before["result"],
                    "obj_bytes",
                ),
            }

            transferred_bytes = first_source_delta["obj_bytes"]
            byte_baseline = dest_ref_bytes + transferred_bytes
            expected_batches = math.ceil(len(preflight_missing) / batch_size)

            result = {
                "scenario": scenario.name,
                "rows": rows_count,
                "seed": seed,
                "configured_receiver_overlap": scenario.receiver_overlap,
                "batch_size": batch_size,
                "objects": {
                    "source_value_ids": len(value_ids),
                    "preseeded_value_ids": len(preseed_ids),
                    "preflight_present_ids": len(preflight_present),
                    "preflight_missing_ids": len(preflight_missing),
                },
                "byte_accounting": {
                    "destination_preexisting_object_bytes": dest_ref_bytes,
                    "source_transferred_object_bytes": transferred_bytes,
                    "object_byte_baseline": byte_baseline,
                    "saved_bytes_ratio": (
                        round(dest_ref_bytes / byte_baseline, 6)
                        if byte_baseline
                        else 0.0
                    ),
                    "saved_pct": pct(dest_ref_bytes, byte_baseline),
                    "analytical_saved_bytes_ratio": analytical["replication"][
                        "saved_bytes_ratio"
                    ],
                },
                "first_pull": {
                    "requested": first_pull["result"].get("requested"),
                    "already_present": first_pull["result"].get("already_present"),
                    "fetched_objects": first_pull["result"].get("fetched_objects"),
                    "stored": first_pull["result"].get("stored"),
                    "batches": first_pull["result"].get("batches"),
                    "expected_batches": expected_batches,
                    "invalid_ids": first_pull["result"].get("invalid_ids") or [],
                    "remote_missing": first_pull["result"].get("remote_missing") or [],
                    "verification_failed": first_pull["result"].get(
                        "verification_failed"
                    )
                    or [],
                },
                "source_fetch": first_source_delta,
                "postflight_missing_ids": len(
                    postflight["result"].get("missing", [])
                ),
                "second_pull": {
                    "requested": second_pull["result"].get("requested"),
                    "already_present": second_pull["result"].get("already_present"),
                    "fetched_objects": second_pull["result"].get("fetched_objects"),
                    "stored": second_pull["result"].get("stored"),
                    "batches": second_pull["result"].get("batches"),
                    "invalid_ids": second_pull["result"].get("invalid_ids") or [],
                    "remote_missing": second_pull["result"].get("remote_missing") or [],
                    "verification_failed": second_pull["result"].get(
                        "verification_failed"
                    )
                    or [],
                },
                "second_pull_source_fetch": second_source_delta,
            }
            validate_scenario_result(result)
            return result
        finally:
            stop_node(dest_proc, dest_handle, dest_url, 999_998)
            stop_node(source_proc, source_handle, source_url, 999_999)
            if keep_logs is not None:
                scenario_dir = keep_logs / scenario.name
                scenario_dir.mkdir(parents=True, exist_ok=True)
                scenario_dir.joinpath("source.log").write_text(
                    source_log.read_text(encoding="utf-8"), encoding="utf-8"
                )
                scenario_dir.joinpath("destination.log").write_text(
                    dest_log.read_text(encoding="utf-8"), encoding="utf-8"
                )


def validate_trends(results: list[dict[str, Any]]) -> None:
    by_name = {result["scenario"]: result for result in results}
    ordered = [
        by_name["low_redundancy_high_churn"],
        by_name["balanced"],
        by_name["high_redundancy_low_churn"],
    ]
    saved = [result["byte_accounting"]["saved_bytes_ratio"] for result in ordered]
    if not saved[0] <= saved[1] <= saved[2]:
        raise RuntimeError(f"two-node CAS byte savings are not monotonic: {saved}")

    for result in ordered:
        if result["first_pull"]["batches"] != result["first_pull"]["expected_batches"]:
            raise RuntimeError(
                f"{result['scenario']}: unexpected objects.pull batch count"
            )


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# SkeinDB Two-Node CAS Transfer Validation",
        "",
        f"Format: `{report['format']}`  ",
        f"Rows per scenario: **{report['rows']}**  ",
        f"Seed: **{report['seed']}**  ",
        f"Pull batch size: **{report['batch_size']}**",
        "",
        "Two independent SkeinDB processes are used. The destination is pre-seeded with a deterministic subset of the source ValueIDs, then calls `objects.pull` against the source over HTTP.",
        "",
        "| Scenario | Source ValueIDs | Pre-seeded | Remote fetched | Stored | Saved object bytes | Transfer batches | Second-pull remote fetches |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in report["results"]:
        lines.append(
            "| {name} | {source} | {preseed} | {fetched} | {stored} | {saved:.1f}% | {batches} | {second} |".format(
                name=result["scenario"],
                source=result["objects"]["source_value_ids"],
                preseed=result["objects"]["preseeded_value_ids"],
                fetched=result["first_pull"]["fetched_objects"],
                stored=result["first_pull"]["stored"],
                saved=result["byte_accounting"]["saved_pct"],
                batches=result["first_pull"]["batches"],
                second=result["second_pull_source_fetch"]["objects_served"],
            )
        )
    lines.extend(
        [
            "",
            "## What is measured",
            "",
            "- The pre-existing byte side is the destination node's real `objects.need` / `cluster.replication_stats.ref_bytes` delta.",
            "- The transferred byte side is the source node's real `objects.fetch` / `cluster.replication_stats.obj_bytes` delta generated by the destination's `objects.pull` call.",
            "- `objects.pull` must report no invalid IDs, remote-missing IDs, or verification failures.",
            "- A post-pull `objects.need` call must find every source ValueID locally.",
            "- A second identical `objects.pull` must make zero source `objects.fetch` calls and transfer zero object bytes.",
            "",
            "## Claim boundary",
            "",
            "The experiment uses a real HTTP path between two SkeinDB processes, but byte accounting is based on ValueStore entry bytes reported by the production counters. It does not include HTTP/TCP framing, TLS, compression, latency, or throughput.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skeindb-bin", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=300)
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--base-port", type=int, default=18380)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    parser.add_argument("--log-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binary = args.skeindb_bin.resolve()
    if not binary.exists():
        raise SystemExit(f"SkeinDB binary does not exist: {binary}")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")

    results = []
    for idx, scenario in enumerate(model.DEFAULT_SCENARIOS):
        source_http = args.base_port + idx * 10
        dest_http = source_http + 1
        source_cluster = source_http + 100
        dest_cluster = source_http + 101
        result = run_scenario(
            binary,
            scenario,
            args.rows,
            args.seed + idx * 101,
            args.batch_size,
            source_http,
            dest_http,
            source_cluster,
            dest_cluster,
            args.log_dir,
        )
        results.append(result)
        print(
            "two-node CAS {name}: preseed={preseed}/{total} fetched={fetched} "
            "saved={saved:.2f}% second_fetch={second}".format(
                name=result["scenario"],
                preseed=result["objects"]["preseeded_value_ids"],
                total=result["objects"]["source_value_ids"],
                fetched=result["first_pull"]["fetched_objects"],
                saved=result["byte_accounting"]["saved_pct"],
                second=result["second_pull_source_fetch"]["objects_served"],
            ),
            flush=True,
        )

    validate_trends(results)
    report = {
        "format": FORMAT,
        "kind": "two_process_real_objects_pull_validation",
        "rows": args.rows,
        "seed": args.seed,
        "batch_size": args.batch_size,
        "claim_boundary": [
            "Two independent SkeinDB processes communicate over the real objects.pull -> objects.fetch HTTP path.",
            "Byte accounting uses production ValueStore entry-byte counters, not a packet capture.",
            "No latency, throughput, HTTP framing, TLS, or compression claim is made.",
        ],
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
