#!/usr/bin/env python3
"""Deterministic cross-layer redundancy evaluation for SkeinDB.

One synthetic relational workload quantifies three related byte-reduction mechanisms:
1. adaptive content-identity refs for storage cell payloads,
2. the CAS replication bound U-I from docs/CAS_REPLICATION.md,
3. query-scoped delta delivery versus a full JSON result.

This is an analytical/reproducibility harness, not a wall-clock benchmark. Runtime ValueIDs
use BLAKE3-128; this script uses a 128-bit stdlib identity surrogate because only equality
and the fixed 32-hex-character identifier width affect these byte counts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import struct
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

FORMAT = "skein.redundancy_pipeline.v1"


@dataclass(frozen=True)
class Scenario:
    name: str
    repetition: float
    receiver_overlap: float
    change_rate: float


DEFAULT_SCENARIOS = (
    Scenario("low_redundancy_high_churn", 0.15, 0.25, 0.20),
    Scenario("balanced", 0.55, 0.60, 0.05),
    Scenario("high_redundancy_low_churn", 0.90, 0.85, 0.01),
)


def compact_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def canonical_cell_bytes(value: Any) -> bytes:
    if value is None:
        return b"\x00"
    if isinstance(value, bool):
        return b"\x01" + (b"\x01" if value else b"\x00")
    if isinstance(value, int):
        return b"\x02" + struct.pack("<q", value)
    if isinstance(value, float):
        return b"\x03" + struct.pack("<d", value)
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        return b"\x04" + struct.pack("<I", len(encoded)) + encoded
    encoded = compact_json_bytes(value)
    return b"\x06" + struct.pack("<I", len(encoded)) + encoded


def identity_token(data: bytes) -> str:
    # Collision-resistant 128-bit surrogate. Runtime uses BLAKE3-128; only identity equality
    # and the 32-hex-character width matter to this deterministic byte model.
    return hashlib.blake2s(data, digest_size=16).hexdigest()


def build_rows(count: int, repetition: float, seed: int) -> list[dict[str, Any]]:
    if count < 2:
        raise ValueError("count must be >= 2")
    if not 0.0 <= repetition <= 1.0:
        raise ValueError("repetition must be within [0, 1]")

    rng = random.Random(seed)
    unique_payloads = max(1, int(round(count * (1.0 - repetition))))
    payloads = []
    for idx in range(unique_payloads):
        salt = "".join(rng.choice("abcdef0123456789") for _ in range(12))
        payloads.append(f"payload-{idx:05d}-{salt}-" + (chr(65 + idx % 26) * 176))

    return [
        {"id": idx + 1, "payload": payloads[idx % unique_payloads], "revision": 1}
        for idx in range(count)
    ]


def _ref_seed_len(token: str, value: Any) -> int:
    return len(compact_json_bytes({"$skein_ref": {"kind": "cell", "id": token, "lit": value}}))


def _ref_compact_len(token: str) -> int:
    return len(compact_json_bytes({"$skein_ref": {"kind": "cell", "id": token}}))


def measure_storage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cells = [
        (identity_token(canonical_cell_bytes(value)), value)
        for row in rows
        for value in row.values()
    ]
    counts: dict[str, int] = {}
    values: dict[str, Any] = {}
    for token, value in cells:
        counts[token] = counts.get(token, 0) + 1
        values.setdefault(token, value)

    use_refs: set[str] = set()
    for token, count in counts.items():
        if count < 2:
            continue
        value = values[token]
        inline_total = len(compact_json_bytes(value)) * count
        ref_total = _ref_seed_len(token, value) + (count - 1) * _ref_compact_len(token)
        if ref_total < inline_total:
            use_refs.add(token)

    inline_bytes = sum(len(compact_json_bytes(value)) for _, value in cells)
    adaptive_bytes = 0
    seen: set[str] = set()
    for token, value in cells:
        if token not in use_refs:
            adaptive_bytes += len(compact_json_bytes(value))
        elif token in seen:
            adaptive_bytes += _ref_compact_len(token)
        else:
            adaptive_bytes += _ref_seed_len(token, value)
            seen.add(token)

    saved = inline_bytes - adaptive_bytes
    return {
        "scope": "cell_value_payload_bytes",
        "inline_bytes": inline_bytes,
        "adaptive_ref_bytes": adaptive_bytes,
        "saved_bytes": saved,
        "savings_pct": round(100.0 * saved / inline_bytes, 3) if inline_bytes else 0.0,
        "distinct_values": len(counts),
        "ref_profitable_values": len(use_refs),
    }


def unique_objects(rows: list[dict[str, Any]]) -> dict[str, bytes]:
    objects: dict[str, bytes] = {}
    for row in rows:
        for value in row.values():
            raw = canonical_cell_bytes(value)
            objects.setdefault(identity_token(raw), raw)
    return objects


def measure_replication(rows: list[dict[str, Any]], overlap: float) -> dict[str, Any]:
    if not 0.0 <= overlap <= 1.0:
        raise ValueError("overlap must be within [0, 1]")
    objects = unique_objects(rows)
    tokens = sorted(objects)
    present = set(tokens[: int(round(len(tokens) * overlap))])
    unique_bytes = sum(len(raw) for raw in objects.values())
    already_present_bytes = sum(len(objects[token]) for token in present)
    transferred_bytes = unique_bytes - already_present_bytes
    return {
        "scope": "unique_value_object_bytes_excluding_row_metadata_and_protocol_overhead",
        "unique_objects": len(objects),
        "receiver_present_objects": len(present),
        "hit_rate": round(len(present) / len(objects), 6) if objects else 0.0,
        "naive_object_bytes": unique_bytes,
        "cas_transfer_bytes": transferred_bytes,
        "saved_bytes": already_present_bytes,
        "saved_bytes_ratio": round(already_present_bytes / unique_bytes, 6) if unique_bytes else 0.0,
    }


def mutate_rows(rows: list[dict[str, Any]], change_rate: float, seed: int) -> list[dict[str, Any]]:
    if not 0.0 <= change_rate <= 1.0:
        raise ValueError("change_rate must be within [0, 1]")
    rng = random.Random(seed)
    current = {row["id"]: dict(row) for row in rows}
    ids = list(current)
    rng.shuffle(ids)
    changes = int(round(len(rows) * change_rate))
    if change_rate > 0 and changes == 0:
        changes = 1

    updates = changes // 2
    removals = changes // 4
    additions = changes - updates - removals
    for pk in ids[:updates]:
        current[pk]["payload"] += f"|update-{pk}"
        current[pk]["revision"] += 1
    for pk in ids[updates : updates + removals]:
        current.pop(pk, None)

    max_id = max(current, default=0)
    templates = list(current.values()) or rows
    for offset in range(additions):
        template = dict(templates[offset % len(templates)])
        new_id = max_id + offset + 1
        template["id"] = new_id
        template["revision"] += 1
        current[new_id] = template
    return [current[pk] for pk in sorted(current)]


def measure_query_delivery(base_rows: list[dict[str, Any]], current_rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = {row["id"]: row for row in base_rows}
    current = {row["id"]: row for row in current_rows}
    current_order = [row["id"] for row in current_rows]
    current_pos = {pk: idx for idx, pk in enumerate(current_order)}

    added = [{"pk": [pk], "at": current_pos[pk], "row": current[pk]} for pk in current_order if pk not in base]
    updated = [
        {"pk": [pk], "at": current_pos[pk], "row": current[pk]}
        for pk in current_order
        if pk in base and base[pk] != current[pk]
    ]
    removed = [{"pk": [pk]} for pk in base if pk not in current]
    patch = {
        "reset": False,
        "base_etag": 'W/"base-0000000000000000"',
        "etag": 'W/"current-0000000000000000"',
        "added": added,
        "updated": updated,
        "removed": removed,
    }
    full = {"etag": patch["etag"], "data": current_rows}
    patch_bytes = len(compact_json_bytes(patch))
    full_bytes = len(compact_json_bytes(full))
    saved = full_bytes - patch_bytes
    return {
        "scope": "compact_json_response_bytes_without_http_compression",
        "full_response_bytes": full_bytes,
        "patch_response_bytes": patch_bytes,
        "patch_saved_bytes": saved,
        "patch_savings_pct": round(100.0 * saved / full_bytes, 3) if full_bytes else 0.0,
        "selected_delivery_mode": "patch" if patch_bytes <= full_bytes else "full",
        "selected_delivery_bytes": min(patch_bytes, full_bytes),
        "added": len(added),
        "updated": len(updated),
        "removed": len(removed),
    }


def run_scenario(scenario: Scenario, rows: int, seed: int) -> dict[str, Any]:
    base = build_rows(rows, scenario.repetition, seed)
    current = mutate_rows(base, scenario.change_rate, seed + 1)
    storage = measure_storage(base)
    replication = measure_replication(base, scenario.receiver_overlap)
    query = measure_query_delivery(base, current)
    baseline = storage["inline_bytes"] + replication["naive_object_bytes"] + query["full_response_bytes"]
    optimized = storage["adaptive_ref_bytes"] + replication["cas_transfer_bytes"] + query["selected_delivery_bytes"]
    saved = baseline - optimized
    return {
        "scenario": asdict(scenario),
        "rows": rows,
        "storage": storage,
        "replication": replication,
        "query_delivery": query,
        "lifecycle_byte_work": {
            "definition": "storage cell payload bytes + replication unique-object bytes + one query-delivery response",
            "baseline_bytes": baseline,
            "redundancy_aware_bytes": optimized,
            "saved_bytes": saved,
            "savings_pct": round(100.0 * saved / baseline, 3) if baseline else 0.0,
        },
    }


def run_benchmark(rows: int = 600, seed: int = 20260922, scenarios: tuple[Scenario, ...] = DEFAULT_SCENARIOS) -> dict[str, Any]:
    return {
        "format": FORMAT,
        "kind": "deterministic_analytical_byte_model",
        "rows": rows,
        "seed": seed,
        "identity_note": "Runtime uses BLAKE3-128 ValueIDs. This harness uses a 128-bit stdlib surrogate because only equality and 32-hex identifier width affect the modeled byte counts.",
        "limitations": [
            "Storage numbers cover serialized cell values, not complete row/file framing.",
            "CAS replication applies the documented U-I object-byte bound and excludes row metadata/protocol overhead.",
            "Query delivery uses compact JSON without HTTP compression and models add/update/remove patches.",
            "Lifecycle byte-work is a cross-layer accounting metric, not bytes on one physical channel.",
        ],
        "results": [run_scenario(scenario, rows, seed + idx * 101) for idx, scenario in enumerate(scenarios)],
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# SkeinDB Redundancy Pipeline Evaluation",
        "",
        f"Format: `{report['format']}`  ",
        f"Rows per scenario: **{report['rows']}**  ",
        f"Seed: **{report['seed']}**",
        "",
        "This deterministic harness uses one workload to account for redundant bytes at three layers: storage, CAS replication, and query delivery.",
        "",
        "| Scenario | Storage saved | CAS object bytes saved | QueryPatch saved | Lifecycle byte-work saved | Delivery |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for result in report["results"]:
        lines.append(
            "| {name} | {storage:.1f}% | {cas:.1f}% | {patch:.1f}% | {life:.1f}% | {mode} |".format(
                name=result["scenario"]["name"],
                storage=result["storage"]["savings_pct"],
                cas=100.0 * result["replication"]["saved_bytes_ratio"],
                patch=result["query_delivery"]["patch_savings_pct"],
                life=result["lifecycle_byte_work"]["savings_pct"],
                mode=result["query_delivery"]["selected_delivery_mode"],
            )
        )
    lines.extend(["", "## Scope and limitations", ""])
    lines.extend(f"- {item}" for item in report["limitations"])
    lines.extend(["", "The JSON report contains the byte counts behind every percentage.", ""])
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=600)
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_benchmark(rows=args.rows, seed=args.seed)
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
