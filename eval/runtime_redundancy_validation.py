#!/usr/bin/env python3
"""Live SkeinDB validation for the cross-layer redundancy pipeline.

Unlike eval/redundancy_pipeline.py, this harness starts the real SkeinDB binary and
collects evidence from runtime RPCs and raw HTTP response bodies.

Runtime evidence sources:
- storage: stats.snapshot.storage logical/unique/duplicate bytes
- CAS object transfer: real objects.need / objects.fetch counters
- query delivery: raw HTTP body bytes for query.patch versus query.select

CAS scope note: the harness is intentionally a single-node receiver-overlap simulation.
It partitions real ValueIDs exposed by skeinpack_v1 into "already present" and "must fetch"
sets. objects.need accounts real saved ValueStore entry bytes and objects.fetch accounts
real transferred entry bytes. This exercises the production RPC/counter path without
claiming a multi-node network throughput measurement.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import redundancy_pipeline as model

FORMAT = "skein.redundancy_pipeline.runtime.v1"


class RpcError(RuntimeError):
    pass


def compact_json(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def rpc(base_url: str, method: str, params: dict[str, Any], request_id: int) -> tuple[dict[str, Any], bytes]:
    payload = compact_json(
        {"skeinql": "1.0", "id": request_id, "method": method, "params": params}
    )
    request = urllib.request.Request(
        base_url + "/api/v1/rpc",
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read()
        raise RpcError(f"{method} HTTP {exc.code}: {body.decode('utf-8', 'replace')}") from exc
    parsed = json.loads(body)
    if not parsed.get("ok", False):
        raise RpcError(f"{method} failed: {json.dumps(parsed, sort_keys=True)}")
    return parsed, body


def lit(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"t": "bool", "v": value}
    if isinstance(value, int):
        return {"t": "i64", "v": value}
    if isinstance(value, float):
        return {"t": "f64", "v": value}
    if isinstance(value, str):
        return {"t": "str", "v": value}
    raise TypeError(f"unsupported runtime literal: {type(value)!r}")


def typed_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: lit(value) for key, value in row.items()}


def query_ast(db: str, table: str) -> dict[str, Any]:
    return {
        "body": {
            "select": {
                "projection": [
                    {"expr": {"col": "id"}},
                    {"expr": {"col": "payload"}},
                    {"expr": {"col": "revision"}},
                ],
                "from": [{"db": db, "table": table}],
            }
        },
        "order_by": [{"expr": {"col": "id"}, "dir": "asc"}],
    }


def id_predicate(pk: int) -> dict[str, Any]:
    return {
        "op": "eq",
        "a": {"col": "id"},
        "b": {"lit": {"t": "i64", "v": pk}},
    }


def wait_ready(base_url: str, proc: subprocess.Popen[str]) -> None:
    deadline = time.time() + 30
    request_id = 1
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"SkeinDB exited before readiness with code {proc.returncode}")
        try:
            rpc(base_url, "system.version", {}, request_id)
            return
        except Exception:
            request_id += 1
            time.sleep(0.2)
    raise RuntimeError("SkeinDB did not become ready within 30 seconds")


def insert_rows(base_url: str, db: str, table: str, rows: list[dict[str, Any]], request_id: int) -> int:
    for start in range(0, len(rows), 100):
        batch = rows[start : start + 100]
        rpc(
            base_url,
            "data.insert",
            {"into": {"db": db, "table": table}, "rows": [typed_row(row) for row in batch]},
            request_id,
        )
        request_id += 1
    return request_id


def apply_mutation(
    base_url: str,
    db: str,
    table: str,
    base_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
    request_id: int,
) -> int:
    base = {row["id"]: row for row in base_rows}
    current = {row["id"]: row for row in current_rows}

    for pk in sorted(set(base) - set(current)):
        rpc(
            base_url,
            "data.delete",
            {"table": {"db": db, "table": table}, "where": id_predicate(pk), "limit": 1, "args": []},
            request_id,
        )
        request_id += 1

    for pk in sorted(set(base) & set(current)):
        if base[pk] == current[pk]:
            continue
        rpc(
            base_url,
            "data.update",
            {
                "table": {"db": db, "table": table},
                "where": id_predicate(pk),
                "set": {
                    "payload": lit(current[pk]["payload"]),
                    "revision": lit(current[pk]["revision"]),
                },
                "limit": 1,
                "args": [],
            },
            request_id,
        )
        request_id += 1

    additions = [current[pk] for pk in sorted(set(current) - set(base))]
    if additions:
        request_id = insert_rows(base_url, db, table, additions, request_id)
    return request_id


def pct(saved: int | float, baseline: int | float) -> float:
    return round((100.0 * saved / baseline), 3) if baseline else 0.0


def fake_missing_ids(real_ids: list[str]) -> list[str]:
    return [
        hashlib.sha256(("runtime-missing:" + value_id).encode("utf-8")).hexdigest()[:32]
        for value_id in real_ids
    ]


def run_runtime_scenario(
    skeindb_bin: Path,
    scenario: model.Scenario,
    rows_count: int,
    seed: int,
    port: int,
    cluster_port: int,
    keep_logs: Path | None = None,
) -> dict[str, Any]:
    base_rows = model.build_rows(rows_count, scenario.repetition, seed)
    current_rows = model.mutate_rows(base_rows, scenario.change_rate, seed + 1)
    analytical = model.run_scenario(scenario, rows_count, seed)

    with tempfile.TemporaryDirectory(prefix="skeindb-redundancy-runtime-") as tmp:
        tmp_path = Path(tmp)
        data_dir = tmp_path / "data"
        log_path = tmp_path / "skeindb.log"
        log_handle = log_path.open("w", encoding="utf-8")
        proc = subprocess.Popen(
            [
                str(skeindb_bin),
                "serve",
                "--data",
                str(data_dir),
                "--http",
                str(port),
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
        base_url = f"http://127.0.0.1:{port}"
        request_id = 10

        try:
            wait_ready(base_url, proc)
            db = "redundancy_eval"
            table = "items"
            rpc(base_url, "schema.create_database", {"db": db}, request_id)
            request_id += 1
            rpc(
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
            request_id += 1
            request_id = insert_rows(base_url, db, table, base_rows, request_id)

            stats, _ = rpc(base_url, "stats.snapshot", {}, request_id)
            request_id += 1
            storage = stats["result"]["storage"]
            logical = int(storage.get("logical_bytes") or 0)
            unique = int(storage.get("unique_bytes") or 0)
            duplicate = int(storage.get("duplicate_bytes") or max(0, logical - unique))

            q = query_ast(db, table)
            base_select, base_select_raw = rpc(
                base_url,
                "query.select",
                {
                    "query": q,
                    "args": [],
                    "result_format": "rows_json",
                    "cache": {"want_etag": True},
                },
                request_id,
            )
            request_id += 1
            base_etag = base_select["result"].get("etag")
            if not base_etag:
                raise RuntimeError("query.select returned no base etag")

            skeinpack, _ = rpc(
                base_url,
                "query.select",
                {
                    "query": q,
                    "args": [],
                    "result_format": "skeinpack_v1",
                    "cache": {"want_etag": True},
                    "wire": {"format": "skeinpack_v1", "known_valueids": []},
                },
                request_id,
            )
            request_id += 1
            dictionary = skeinpack["result"].get("data", {}).get("dict", {})
            value_ids = sorted(
                value_id
                for value_id in dictionary
                if isinstance(value_id, str) and len(value_id) == 32
            )
            if len(value_ids) < 2:
                raise RuntimeError(f"skeinpack_v1 exposed too few ValueIDs: {len(value_ids)}")

            present_count = int(round(len(value_ids) * scenario.receiver_overlap))
            present_count = max(1, min(len(value_ids) - 1, present_count))
            already_present_ids = value_ids[:present_count]
            transfer_ids = value_ids[present_count:]
            need_ids = already_present_ids + fake_missing_ids(transfer_ids)

            need, _ = rpc(base_url, "objects.need", {"ids": need_ids}, request_id)
            request_id += 1
            fetched, _ = rpc(base_url, "objects.fetch", {"ids": transfer_ids}, request_id)
            request_id += 1
            rep_stats, _ = rpc(base_url, "cluster.replication_stats", {}, request_id)
            request_id += 1
            rep = rep_stats["result"]

            if len(need["result"].get("present", [])) != len(already_present_ids):
                raise RuntimeError("objects.need did not recognize expected present ValueIDs")
            if len(fetched["result"].get("objects", [])) != len(transfer_ids):
                raise RuntimeError("objects.fetch did not serve expected ValueIDs")

            request_id = apply_mutation(
                base_url, db, table, base_rows, current_rows, request_id
            )

            patch, patch_raw = rpc(
                base_url,
                "query.patch",
                {
                    "query": q,
                    "args": [],
                    "base_etag": base_etag,
                    "result_format": "rows_json",
                    "include_full": False,
                },
                request_id,
            )
            request_id += 1
            patch_data = patch["result"].get("data") or {}
            if patch_data.get("reset"):
                raise RuntimeError(
                    "query.patch reset instead of producing delta: "
                    + json.dumps(patch_data, sort_keys=True)
                )

            full, full_raw = rpc(
                base_url,
                "query.select",
                {
                    "query": q,
                    "args": [],
                    "result_format": "rows_json",
                    "cache": {"want_etag": True},
                },
                request_id,
            )
            request_id += 1
            if full["result"].get("not_modified"):
                raise RuntimeError("fresh full query unexpectedly returned not_modified")

            patch_saved = len(full_raw) - len(patch_raw)
            runtime_result = {
                "scenario": scenario.name,
                "rows": rows_count,
                "seed": seed,
                "storage": {
                    "source": "stats.snapshot.storage",
                    "logical_bytes": logical,
                    "unique_bytes": unique,
                    "duplicate_bytes": duplicate,
                    "savings_pct": pct(logical - unique, logical),
                    "dedup_ratio": storage.get("dedup_ratio"),
                    "interned_values": storage.get("interned_values"),
                    "unique_values": storage.get("unique_values"),
                    "disk_bytes": storage.get("disk_bytes"),
                },
                "cas_replication": {
                    "source": "objects.need + objects.fetch + cluster.replication_stats",
                    "scope": "single-node receiver-overlap simulation over real skeinpack ValueIDs",
                    "value_ids": len(value_ids),
                    "configured_receiver_overlap": scenario.receiver_overlap,
                    "already_present_ids": len(already_present_ids),
                    "fetched_ids": len(transfer_ids),
                    "need_hits": rep.get("need_hits"),
                    "need_misses": rep.get("need_misses"),
                    "fetch_objects_served": rep.get("fetch_objects_served"),
                    "ref_bytes": rep.get("ref_bytes"),
                    "obj_bytes": rep.get("obj_bytes"),
                    "saved_bytes": rep.get("saved_bytes"),
                    "hit_rate": rep.get("hit_rate"),
                    "saved_bytes_ratio": rep.get("saved_bytes_ratio"),
                },
                "query_delivery": {
                    "source": "raw /api/v1/rpc HTTP response bodies",
                    "base_select_bytes": len(base_select_raw),
                    "full_response_bytes": len(full_raw),
                    "patch_response_bytes": len(patch_raw),
                    "patch_saved_bytes": patch_saved,
                    "patch_savings_pct": pct(patch_saved, len(full_raw)),
                    "base_source": patch_data.get("base_source"),
                    "added": len(patch_data.get("added") or []),
                    "updated": len(patch_data.get("updated") or []),
                    "removed": len(patch_data.get("removed") or []),
                },
                "analytical_reference": {
                    "storage_savings_pct": analytical["storage"]["savings_pct"],
                    "cas_saved_bytes_ratio": analytical["replication"]["saved_bytes_ratio"],
                    "query_patch_savings_pct": analytical["query_delivery"]["patch_savings_pct"],
                    "lifecycle_savings_pct": analytical["lifecycle_byte_work"]["savings_pct"],
                },
            }
            runtime_result["model_delta_pct_points"] = {
                "storage": round(
                    runtime_result["storage"]["savings_pct"]
                    - analytical["storage"]["savings_pct"],
                    3,
                ),
                "cas": round(
                    100.0 * float(runtime_result["cas_replication"]["saved_bytes_ratio"] or 0)
                    - 100.0 * analytical["replication"]["saved_bytes_ratio"],
                    3,
                ),
                "query_patch": round(
                    runtime_result["query_delivery"]["patch_savings_pct"]
                    - analytical["query_delivery"]["patch_savings_pct"],
                    3,
                ),
            }
            return runtime_result
        finally:
            try:
                if proc.poll() is None:
                    rpc(base_url, "system.shutdown", {}, 999999)
                    proc.wait(timeout=5)
            except Exception:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=3)
            log_handle.close()
            if keep_logs is not None:
                keep_logs.mkdir(parents=True, exist_ok=True)
                target = keep_logs / f"{scenario.name}.log"
                target.write_text(log_path.read_text(encoding="utf-8"), encoding="utf-8")


def validate_trends(results: list[dict[str, Any]]) -> None:
    by_name = {result["scenario"]: result for result in results}
    low = by_name["low_redundancy_high_churn"]
    balanced = by_name["balanced"]
    high = by_name["high_redundancy_low_churn"]

    storage = [x["storage"]["savings_pct"] for x in (low, balanced, high)]
    cas = [
        float(x["cas_replication"]["saved_bytes_ratio"] or 0)
        for x in (low, balanced, high)
    ]
    query = [x["query_delivery"]["patch_savings_pct"] for x in (low, balanced, high)]

    if not storage[0] <= storage[1] <= storage[2]:
        raise RuntimeError(f"runtime storage savings are not monotonic: {storage}")
    if not cas[0] <= cas[1] <= cas[2]:
        raise RuntimeError(f"runtime CAS savings are not monotonic: {cas}")
    if not query[0] <= query[1] <= query[2]:
        raise RuntimeError(f"runtime QueryPatch savings are not monotonic: {query}")
    if any(x["query_delivery"]["patch_saved_bytes"] <= 0 for x in results):
        raise RuntimeError("at least one runtime QueryPatch response was not smaller than full")


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# SkeinDB Runtime Redundancy Validation",
        "",
        f"Format: `{report['format']}`  ",
        f"Rows per scenario: **{report['rows']}**  ",
        f"Seed: **{report['seed']}**",
        "",
        "This report comes from a real SkeinDB process and real RPC response bodies.",
        "",
        "| Scenario | Runtime storage saved | Runtime CAS bytes saved | Runtime QueryPatch saved | Model storage | Model CAS | Model QueryPatch |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for result in report["results"]:
        ref = result["analytical_reference"]
        lines.append(
            "| {name} | {storage:.1f}% | {cas:.1f}% | {query:.1f}% | {mstorage:.1f}% | {mcas:.1f}% | {mquery:.1f}% |".format(
                name=result["scenario"],
                storage=result["storage"]["savings_pct"],
                cas=100.0 * float(result["cas_replication"]["saved_bytes_ratio"] or 0),
                query=result["query_delivery"]["patch_savings_pct"],
                mstorage=ref["storage_savings_pct"],
                mcas=100.0 * ref["cas_saved_bytes_ratio"],
                mquery=ref["query_patch_savings_pct"],
            )
        )
    lines.extend(
        [
            "",
            "## Evidence scope",
            "",
            "- Storage is read directly from `stats.snapshot.storage`.",
            "- CAS bytes are real ValueStore entry bytes counted by `objects.need` and `objects.fetch`; receiver overlap is simulated on one node, so this is not a network-throughput result.",
            "- Query delivery counts the raw HTTP body bytes returned by `/api/v1/rpc` for `query.patch` and a full `query.select`.",
            "- The analytical reference uses the same scenario parameters and seed.",
            "",
        ]
    )

    cas_deltas = [result["model_delta_pct_points"]["cas"] for result in report["results"]]
    query_deltas = [
        result["model_delta_pct_points"]["query_patch"] for result in report["results"]
    ]
    lines.extend(
        [
            "## Interpretation",
            "",
            "- **CAS calibration is close.** Runtime CAS savings differ from the analytical model by "
            + ", ".join(f"{value:+.1f}" for value in cas_deltas)
            + " percentage points across the three scenarios.",
            "- **QueryPatch has fixed protocol overhead that matters at higher churn.** The analytical model counts compact patch payloads, while the runtime measurement includes the full RPC envelope, typed literals, columns, dependencies, causality, and patch metadata. The gap shrinks from "
            + f"{query_deltas[0]:.1f} points at high churn to {query_deltas[-1]:.1f} points at low churn.",
            "- **Storage uses a different denominator by design.** Runtime `stats.snapshot.storage` reports logical ValueStore bytes versus unique ValueStore bytes, while the analytical model estimates adaptive on-disk cell-reference encoding. The runtime percentages therefore should not be treated as a direct validation of the encoding model.",
            "",
            "The full JSON snapshot contains the byte counts and model deltas behind these percentages.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skeindb-bin", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=300)
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument("--base-port", type=int, default=18180)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    parser.add_argument("--log-dir", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binary = args.skeindb_bin.resolve()
    if not binary.exists():
        raise SystemExit(f"SkeinDB binary does not exist: {binary}")

    results = []
    for idx, scenario in enumerate(model.DEFAULT_SCENARIOS):
        result = run_runtime_scenario(
            binary,
            scenario,
            args.rows,
            args.seed + idx * 101,
            args.base_port + idx,
            args.base_port + 100 + idx,
            args.log_dir,
        )
        results.append(result)
        print(
            "runtime scenario {name}: storage={storage:.2f}% cas={cas:.2f}% patch={patch:.2f}%".format(
                name=scenario.name,
                storage=result["storage"]["savings_pct"],
                cas=100.0 * float(result["cas_replication"]["saved_bytes_ratio"] or 0),
                patch=result["query_delivery"]["patch_savings_pct"],
            ),
            flush=True,
        )

    validate_trends(results)
    report = {
        "format": FORMAT,
        "kind": "live_engine_byte_validation",
        "rows": args.rows,
        "seed": args.seed,
        "claim_boundary": [
            "No wall-clock throughput or latency claim is made.",
            "CAS overlap is simulated on one node, but saved/fetched bytes come from real ValueStore RPC counters.",
            "Query byte counts are real HTTP response body sizes without transport compression.",
            "Storage byte counters are the live stats.snapshot values reported by the engine.",
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
