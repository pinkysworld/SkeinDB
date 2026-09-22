#!/usr/bin/env python3
"""Measure real HTTP-over-TCP bytes for SkeinDB two-node CAS pulls.

The experiment compares two destination nodes against the same source:

1. zero-overlap baseline: destination has none of the source ValueIDs
2. CAS-overlap case: destination is pre-seeded with the scenario's configured
   fraction of the exact source ValueIDs

Both destinations use the production objects.pull -> objects.fetch HTTP path.
A transparent TCP proxy sits between destination and source and counts every
TCP payload byte in each direction. This includes the HTTP request/status lines,
headers, JSON-RPC envelopes, ValueID strings, Base64 fields, and JSON syntax.

It does not include Ethernet/IP/TCP packet headers or TLS because the CI path is
plain loopback HTTP. Those boundaries are explicit in the report.
"""

from __future__ import annotations

import argparse
import json
import selectors
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import redundancy_pipeline as model
import runtime_redundancy_validation as runtime
import two_node_cas_validation as cas

FORMAT = "skein.cas_http_wire.runtime.v1"


class WireCounters:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._client_to_source = 0
        self._source_to_client = 0
        self._connections = 0
        self._active = 0

    def begin(self) -> None:
        with self._cond:
            self._connections += 1
            self._active += 1

    def add_client_to_source(self, size: int) -> None:
        with self._lock:
            self._client_to_source += size

    def add_source_to_client(self, size: int) -> None:
        with self._lock:
            self._source_to_client += size

    def end(self) -> None:
        with self._cond:
            self._active -= 1
            self._cond.notify_all()

    def wait_idle(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._active:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"wire proxy still has {self._active} active connection(s)"
                    )
                self._cond.wait(timeout=remaining)

    def reset(self) -> None:
        self.wait_idle()
        with self._lock:
            self._client_to_source = 0
            self._source_to_client = 0
            self._connections = 0

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            client_to_source = self._client_to_source
            source_to_client = self._source_to_client
            connections = self._connections
            active = self._active
        return {
            "connections": connections,
            "active_connections": active,
            "client_to_source_bytes": client_to_source,
            "source_to_client_bytes": source_to_client,
            "total_http_tcp_payload_bytes": client_to_source + source_to_client,
        }


class _CountingProxyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        target_address: tuple[str, int],
        counters: WireCounters,
    ) -> None:
        self.target_address = target_address
        self.counters = counters
        super().__init__(server_address, _CountingProxyHandler)


class _CountingProxyHandler(socketserver.BaseRequestHandler):
    server: _CountingProxyServer

    def handle(self) -> None:
        counters = self.server.counters
        counters.begin()
        upstream: socket.socket | None = None
        try:
            upstream = socket.create_connection(self.server.target_address, timeout=5)
            client = self.request
            client.settimeout(None)
            upstream.settimeout(None)

            selector = selectors.DefaultSelector()
            selector.register(client, selectors.EVENT_READ, ("c2s", upstream))
            selector.register(upstream, selectors.EVENT_READ, ("s2c", client))

            while selector.get_map():
                events = selector.select(timeout=10)
                if not events:
                    raise TimeoutError("wire proxy relay timed out")
                for key, _ in events:
                    sock = key.fileobj
                    direction, peer = key.data
                    try:
                        data = sock.recv(65536)
                    except ConnectionResetError:
                        data = b""
                    if not data:
                        try:
                            selector.unregister(sock)
                        except Exception:
                            pass
                        try:
                            peer.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass
                        continue
                    peer.sendall(data)
                    if direction == "c2s":
                        counters.add_client_to_source(len(data))
                    else:
                        counters.add_source_to_client(len(data))
            selector.close()
        finally:
            if upstream is not None:
                try:
                    upstream.close()
                except OSError:
                    pass
            counters.end()


class CountingTCPProxy:
    def __init__(self, listen_port: int, target_port: int) -> None:
        self.counters = WireCounters()
        self._server = _CountingProxyServer(
            ("127.0.0.1", listen_port),
            ("127.0.0.1", target_port),
            self.counters,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"skeindb-wire-proxy-{listen_port}",
            daemon=True,
        )
        self.listen_port = listen_port

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.listen_port}"

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def reset(self) -> None:
        self.counters.reset()

    def snapshot(self) -> dict[str, int]:
        self.counters.wait_idle()
        return self.counters.snapshot()


def source_fetch_delta(
    before: dict[str, Any], after: dict[str, Any]
) -> dict[str, int]:
    return {
        "fetch_calls": cas.delta(after, before, "fetch_calls"),
        "fetch_ids_total": cas.delta(after, before, "fetch_ids_total"),
        "objects_served": cas.delta(after, before, "fetch_objects_served"),
        "obj_bytes": cas.delta(after, before, "obj_bytes"),
    }


def validate_pull(label: str, pull: dict[str, Any]) -> None:
    result = pull["result"]
    if result.get("invalid_ids") or result.get("remote_missing") or result.get(
        "verification_failed"
    ):
        raise RuntimeError(f"{label}: objects.pull reported integrity failures")


def pull_with_measurement(
    destination_url: str,
    source_stats_url: str,
    proxy: CountingTCPProxy,
    ids: list[str],
    batch_size: int,
    request_id: int,
    source_request_id: int,
) -> tuple[dict[str, Any], dict[str, int], dict[str, int], int, int]:
    proxy.reset()
    before, _ = runtime.rpc(
        source_stats_url, "cluster.replication_stats", {}, source_request_id
    )
    source_request_id += 1

    pull, _ = runtime.rpc(
        destination_url,
        "objects.pull",
        {
            "source_rpc_url": proxy.url,
            "ids": ids,
            "batch_size": batch_size,
        },
        request_id,
    )
    request_id += 1
    proxy.counters.wait_idle(timeout=10)
    wire = proxy.snapshot()

    after, _ = runtime.rpc(
        source_stats_url, "cluster.replication_stats", {}, source_request_id
    )
    source_request_id += 1
    fetch = source_fetch_delta(before["result"], after["result"])
    return pull, fetch, wire, request_id, source_request_id


def transfer_metrics(
    fetch: dict[str, int],
    wire: dict[str, int],
) -> dict[str, Any]:
    obj_bytes = fetch["obj_bytes"]
    total = wire["total_http_tcp_payload_bytes"]
    downstream = wire["source_to_client_bytes"]
    return {
        "value_store_object_bytes": obj_bytes,
        "http_tcp_payload_bytes": total,
        "request_bytes": wire["client_to_source_bytes"],
        "response_bytes": downstream,
        "connections": wire["connections"],
        "http_overhead_bytes_vs_value_store": total - obj_bytes,
        "total_wire_expansion_ratio": round(total / obj_bytes, 6)
        if obj_bytes
        else 0.0,
        "response_expansion_ratio": round(downstream / obj_bytes, 6)
        if obj_bytes
        else 0.0,
        "value_store_efficiency_pct": cas.pct(obj_bytes, total),
    }


def run_scenario(
    skeindb_bin: Path,
    scenario: model.Scenario,
    rows_count: int,
    seed: int,
    batch_size: int,
    source_http_port: int,
    baseline_http_port: int,
    overlap_http_port: int,
    proxy_port: int,
    source_cluster_port: int,
    baseline_cluster_port: int,
    overlap_cluster_port: int,
    keep_logs: Path | None,
) -> dict[str, Any]:
    source_rows = model.build_rows(rows_count, scenario.repetition, seed)

    with tempfile.TemporaryDirectory(prefix="skeindb-cas-http-wire-") as tmp:
        root = Path(tmp)
        source_log = root / "source.log"
        baseline_log = root / "baseline-destination.log"
        overlap_log = root / "overlap-destination.log"

        source_proc, source_handle, source_url = cas.launch_node(
            skeindb_bin,
            root / "source-data",
            source_http_port,
            source_cluster_port,
            source_log,
        )
        baseline_proc, baseline_handle, baseline_url = cas.launch_node(
            skeindb_bin,
            root / "baseline-data",
            baseline_http_port,
            baseline_cluster_port,
            baseline_log,
        )
        overlap_proc, overlap_handle, overlap_url = cas.launch_node(
            skeindb_bin,
            root / "overlap-data",
            overlap_http_port,
            overlap_cluster_port,
            overlap_log,
        )
        proxy = CountingTCPProxy(proxy_port, source_http_port)
        proxy.start()

        source_request = 100
        baseline_request = 20_000
        overlap_request = 30_000

        try:
            source_request = cas.create_table(
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

            # CR05 compatibility contract: direct objects.fetch remains legacy by
            # default, while transfer_only returns only the fields required by
            # objects.pull. The canonical transfer payload itself must be identical.
            probe_id = value_ids[0]
            legacy_fetch, _ = runtime.rpc(
                source_url,
                "objects.fetch",
                {"ids": [probe_id]},
                source_request,
            )
            source_request += 1
            compact_fetch, _ = runtime.rpc(
                source_url,
                "objects.fetch",
                {"ids": [probe_id], "transfer_only": True},
                source_request,
            )
            source_request += 1
            legacy_object = legacy_fetch["result"]["objects"][0]
            compact_object = compact_fetch["result"]["objects"][0]
            required_legacy = {"id", "bytes_b64", "entry_b64", "kind", "verified"}
            if not required_legacy.issubset(legacy_object):
                raise RuntimeError(
                    f"{scenario.name}: legacy objects.fetch shape changed: "
                    f"{sorted(legacy_object)}"
                )
            if set(compact_object) != {"id", "entry_b64"}:
                raise RuntimeError(
                    f"{scenario.name}: compact objects.fetch shape unexpected: "
                    f"{sorted(compact_object)}"
                )
            if compact_object["entry_b64"] != legacy_object["entry_b64"]:
                raise RuntimeError(
                    f"{scenario.name}: compact transfer payload differs from legacy"
                )

            baseline_pull, baseline_fetch, baseline_wire, baseline_request, source_request = (
                pull_with_measurement(
                    baseline_url,
                    source_url,
                    proxy,
                    value_ids,
                    batch_size,
                    baseline_request,
                    source_request,
                )
            )
            validate_pull(f"{scenario.name} baseline", baseline_pull)
            baseline_postflight, _ = runtime.rpc(
                baseline_url, "objects.need", {"ids": value_ids}, baseline_request
            )
            baseline_request += 1
            if baseline_postflight["result"].get("missing"):
                raise RuntimeError(
                    f"{scenario.name}: baseline destination still misses objects"
                )

            target_preseed = int(round(len(value_ids) * scenario.receiver_overlap))
            target_preseed = max(1, min(len(value_ids) - 1, target_preseed))
            preseed_ids = value_ids[:target_preseed]
            overlap_request = cas.create_table(
                overlap_url,
                "cas_destination",
                "seed_values",
                overlap_request,
            )
            overlap_request = cas.insert_literal_rows(
                overlap_url,
                "cas_destination",
                "seed_values",
                cas.prefill_rows(preseed_ids, dictionary),
                overlap_request,
            )
            preflight, _ = runtime.rpc(
                overlap_url, "objects.need", {"ids": value_ids}, overlap_request
            )
            overlap_request += 1
            present = preflight["result"].get("present", [])
            missing = preflight["result"].get("missing", [])
            if len(present) != target_preseed:
                raise RuntimeError(
                    f"{scenario.name}: expected {target_preseed} pre-seeded ValueIDs, "
                    f"found {len(present)}"
                )

            overlap_pull, overlap_fetch, overlap_wire, overlap_request, source_request = (
                pull_with_measurement(
                    overlap_url,
                    source_url,
                    proxy,
                    value_ids,
                    batch_size,
                    overlap_request,
                    source_request,
                )
            )
            validate_pull(f"{scenario.name} overlap", overlap_pull)
            overlap_postflight, _ = runtime.rpc(
                overlap_url, "objects.need", {"ids": value_ids}, overlap_request
            )
            overlap_request += 1
            if overlap_postflight["result"].get("missing"):
                raise RuntimeError(
                    f"{scenario.name}: overlap destination still misses objects"
                )

            proxy.reset()
            second_pull, second_fetch, second_wire, overlap_request, source_request = (
                pull_with_measurement(
                    overlap_url,
                    source_url,
                    proxy,
                    value_ids,
                    batch_size,
                    overlap_request,
                    source_request,
                )
            )
            validate_pull(f"{scenario.name} second pull", second_pull)
            if (
                second_pull["result"].get("fetched_objects") != 0
                or second_fetch["objects_served"] != 0
                or second_wire["total_http_tcp_payload_bytes"] != 0
                or second_wire["connections"] != 0
            ):
                raise RuntimeError(
                    f"{scenario.name}: idempotent pull unexpectedly used remote HTTP"
                )

            baseline = transfer_metrics(baseline_fetch, baseline_wire)
            overlap = transfer_metrics(overlap_fetch, overlap_wire)
            object_saved = (
                baseline["value_store_object_bytes"]
                - overlap["value_store_object_bytes"]
            )
            wire_saved = (
                baseline["http_tcp_payload_bytes"]
                - overlap["http_tcp_payload_bytes"]
            )

            result = {
                "scenario": scenario.name,
                "rows": rows_count,
                "seed": seed,
                "batch_size": batch_size,
                "configured_receiver_overlap": scenario.receiver_overlap,
                "objects": {
                    "source_value_ids": len(value_ids),
                    "preseeded_value_ids": len(preseed_ids),
                    "overlap_missing_value_ids": len(missing),
                },
                "fetch_format_compatibility": {
                    "legacy_fields": sorted(legacy_object),
                    "transfer_only_fields": sorted(compact_object),
                    "entry_payload_equal": (
                        compact_object["entry_b64"] == legacy_object["entry_b64"]
                    ),
                },
                "zero_overlap_baseline": {
                    "pull": {
                        "batches": baseline_pull["result"].get("batches"),
                        "fetched_objects": baseline_pull["result"].get(
                            "fetched_objects"
                        ),
                        "stored": baseline_pull["result"].get("stored"),
                    },
                    "fetch": baseline_fetch,
                    "wire": baseline,
                },
                "cas_overlap": {
                    "pull": {
                        "already_present": overlap_pull["result"].get(
                            "already_present"
                        ),
                        "batches": overlap_pull["result"].get("batches"),
                        "fetched_objects": overlap_pull["result"].get(
                            "fetched_objects"
                        ),
                        "stored": overlap_pull["result"].get("stored"),
                    },
                    "fetch": overlap_fetch,
                    "wire": overlap,
                },
                "savings": {
                    "value_store_object_bytes_saved": object_saved,
                    "value_store_object_savings_pct": cas.pct(
                        object_saved, baseline["value_store_object_bytes"]
                    ),
                    "http_tcp_payload_bytes_saved": wire_saved,
                    "http_tcp_payload_savings_pct": cas.pct(
                        wire_saved, baseline["http_tcp_payload_bytes"]
                    ),
                    "wire_savings_minus_object_savings_points": round(
                        cas.pct(wire_saved, baseline["http_tcp_payload_bytes"])
                        - cas.pct(
                            object_saved,
                            baseline["value_store_object_bytes"],
                        ),
                        3,
                    ),
                },
                "second_pull": {
                    "already_present": second_pull["result"].get("already_present"),
                    "fetched_objects": second_pull["result"].get("fetched_objects"),
                    "source_objects_served": second_fetch["objects_served"],
                    "http_tcp_payload_bytes": second_wire[
                        "total_http_tcp_payload_bytes"
                    ],
                    "connections": second_wire["connections"],
                },
            }
            validate_result(result)
            return result
        finally:
            proxy.stop()
            cas.stop_node(overlap_proc, overlap_handle, overlap_url, 999_997)
            cas.stop_node(baseline_proc, baseline_handle, baseline_url, 999_998)
            cas.stop_node(source_proc, source_handle, source_url, 999_999)
            if keep_logs is not None:
                scenario_dir = keep_logs / scenario.name
                scenario_dir.mkdir(parents=True, exist_ok=True)
                for source_path, target_name in (
                    (source_log, "source.log"),
                    (baseline_log, "baseline-destination.log"),
                    (overlap_log, "overlap-destination.log"),
                ):
                    scenario_dir.joinpath(target_name).write_text(
                        source_path.read_text(encoding="utf-8"),
                        encoding="utf-8",
                    )


def validate_result(result: dict[str, Any]) -> None:
    baseline = result["zero_overlap_baseline"]
    overlap = result["cas_overlap"]
    second = result["second_pull"]
    objects = result["objects"]

    if baseline["pull"]["fetched_objects"] < objects["source_value_ids"]:
        raise RuntimeError(
            f"{result['scenario']}: zero-overlap baseline fetched too few objects"
        )
    if overlap["pull"]["already_present"] != objects["preseeded_value_ids"]:
        raise RuntimeError(
            f"{result['scenario']}: overlap pull already_present mismatch"
        )
    if overlap["pull"]["fetched_objects"] != objects["overlap_missing_value_ids"]:
        raise RuntimeError(
            f"{result['scenario']}: overlap pull fetched-object mismatch"
        )
    if baseline["wire"]["http_tcp_payload_bytes"] <= 0:
        raise RuntimeError(f"{result['scenario']}: baseline measured zero HTTP bytes")
    if overlap["wire"]["http_tcp_payload_bytes"] <= 0:
        raise RuntimeError(f"{result['scenario']}: overlap measured zero HTTP bytes")
    if (
        overlap["wire"]["http_tcp_payload_bytes"]
        >= baseline["wire"]["http_tcp_payload_bytes"]
    ):
        raise RuntimeError(
            f"{result['scenario']}: CAS overlap did not reduce HTTP transfer bytes"
        )
    if result["savings"]["http_tcp_payload_savings_pct"] <= 0:
        raise RuntimeError(f"{result['scenario']}: HTTP savings are not positive")
    if (
        second["fetched_objects"] != 0
        or second["source_objects_served"] != 0
        or second["http_tcp_payload_bytes"] != 0
        or second["connections"] != 0
    ):
        raise RuntimeError(
            f"{result['scenario']}: second pull was not wire-idempotent"
        )


def validate_trends(results: list[dict[str, Any]]) -> None:
    by_name = {result["scenario"]: result for result in results}
    ordered = [
        by_name["low_redundancy_high_churn"],
        by_name["balanced"],
        by_name["high_redundancy_low_churn"],
    ]
    wire = [x["savings"]["http_tcp_payload_savings_pct"] for x in ordered]
    if not wire[0] <= wire[1] <= wire[2]:
        raise RuntimeError(f"HTTP wire savings are not monotonic: {wire}")


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# SkeinDB Two-Node CAS HTTP Wire Validation",
        "",
        f"Format: `{report['format']}`  ",
        f"Rows per scenario: **{report['rows']}**  ",
        f"Seed: **{report['seed']}**  ",
        f"Pull batch size: **{report['batch_size']}**",
        "",
        "A transparent TCP proxy counts the exact TCP payload bytes used by the production HTTP objects.pull -> objects.fetch path. Each scenario compares a zero-overlap destination with a destination pre-seeded at the configured CAS overlap.",
        "",
        "| Scenario | Baseline HTTP bytes | CAS HTTP bytes | HTTP bytes saved | Object bytes saved | Baseline wire / object | CAS wire / object | Second-pull HTTP bytes |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in report["results"]:
        baseline = result["zero_overlap_baseline"]["wire"]
        overlap = result["cas_overlap"]["wire"]
        lines.append(
            "| {name} | {baseline} | {overlap} | {wire_saved:.1f}% | {obj_saved:.1f}% | {base_exp:.2f}x | {cas_exp:.2f}x | {second} |".format(
                name=result["scenario"],
                baseline=baseline["http_tcp_payload_bytes"],
                overlap=overlap["http_tcp_payload_bytes"],
                wire_saved=result["savings"]["http_tcp_payload_savings_pct"],
                obj_saved=result["savings"]["value_store_object_savings_pct"],
                base_exp=baseline["total_wire_expansion_ratio"],
                cas_exp=overlap["total_wire_expansion_ratio"],
                second=result["second_pull"]["http_tcp_payload_bytes"],
            )
        )
    lines.extend(
        [
            "",
            "## What the byte counter includes",
            "",
            "- HTTP request line and request headers",
            "- JSON-RPC request bodies containing ValueIDs",
            "- HTTP status line and response headers",
            "- JSON-RPC response envelopes",
            "- Base64-encoded `entry_b64` transfer payloads used by `objects.pull`",
            "- all other JSON syntax and metadata carried over the loopback TCP stream",
            "- the legacy `bytes_b64` field is compatibility-tested separately and is not present in the measured CR05 pull traffic",
            "",
            "## What it excludes",
            "",
            "- Ethernet, IP, and TCP packet headers",
            "- retransmission accounting below the TCP stream",
            "- TLS framing, because the CI experiment uses plain loopback HTTP",
            "- latency and throughput claims",
            "",
            "The report therefore measures exact **HTTP-over-TCP payload bytes** seen by the transparent proxy, not packet-capture bytes on a physical network.",
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
    parser.add_argument("--base-port", type=int, default=18680)
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
        base = args.base_port + idx * 20
        result = run_scenario(
            binary,
            scenario,
            args.rows,
            args.seed + idx * 101,
            args.batch_size,
            source_http_port=base,
            baseline_http_port=base + 1,
            overlap_http_port=base + 2,
            proxy_port=base + 3,
            source_cluster_port=base + 100,
            baseline_cluster_port=base + 101,
            overlap_cluster_port=base + 102,
            keep_logs=args.log_dir,
        )
        results.append(result)
        print(
            "HTTP wire {name}: baseline={baseline}B cas={cas_wire}B "
            "wire_saved={saved:.2f}% object_saved={obj:.2f}% second={second}B".format(
                name=result["scenario"],
                baseline=result["zero_overlap_baseline"]["wire"][
                    "http_tcp_payload_bytes"
                ],
                cas_wire=result["cas_overlap"]["wire"][
                    "http_tcp_payload_bytes"
                ],
                saved=result["savings"]["http_tcp_payload_savings_pct"],
                obj=result["savings"]["value_store_object_savings_pct"],
                second=result["second_pull"]["http_tcp_payload_bytes"],
            ),
            flush=True,
        )

    validate_trends(results)
    report = {
        "format": FORMAT,
        "kind": "raw_tcp_proxy_http_transfer_measurement",
        "rows": args.rows,
        "seed": args.seed,
        "batch_size": args.batch_size,
        "claim_boundary": [
            "Counts exact TCP payload bytes between objects.pull and objects.fetch over plain HTTP.",
            "Includes HTTP framing, JSON-RPC envelopes, ValueID request data, Base64 payloads, and JSON metadata.",
            "Excludes Ethernet/IP/TCP packet headers, lower-layer retransmission accounting, and TLS.",
            "No latency or throughput claim is made.",
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
