#!/usr/bin/env python3
"""CR08: decompose SkeinDB CAS replication HTTP-wire bytes.

The existing two-node wire benchmark tells us *how many* bytes cross the
objects.pull -> objects.fetch path. This evaluator explains where those bytes go.

For each measured pull it records the exact TCP payload in both directions,
parses the HTTP/1.1 messages, and partitions the wire bytes into:

request side
- HTTP headers / start line
- HTTP transfer framing
- JSON representation of the ValueID array
- remaining JSON-RPC request envelope

response side
- HTTP headers / status line
- HTTP transfer framing
- CR09 binary envelope framing
- canonical transfer-entry bytes

For backward compatibility, the parser also understands the pre-CR09
JSON/entry_b64 response. It compares decoded/canonical transfer bytes to
SkeinDB's source-side ValueStore obj_bytes counter.

This is an explanatory byte-accounting experiment, not a latency/throughput test.
"""

from __future__ import annotations

import argparse
import base64
import json
import selectors
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redundancy_pipeline as model
import runtime_redundancy_validation as runtime
import two_node_cas_validation as cas

FORMAT = "skein.cas_wire_composition.runtime.v1"


def compact_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


class CaptureCounters:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._connections = 0
        self._active = 0
        self._client_to_source = bytearray()
        self._source_to_client = bytearray()

    def begin(self) -> None:
        with self._cond:
            self._connections += 1
            self._active += 1

    def add_client_to_source(self, data: bytes) -> None:
        with self._lock:
            self._client_to_source.extend(data)

    def add_source_to_client(self, data: bytes) -> None:
        with self._lock:
            self._source_to_client.extend(data)

    def end(self) -> None:
        with self._cond:
            self._active -= 1
            self._cond.notify_all()

    def wait_idle(self, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._active:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"capture proxy still has {self._active} active connection(s)"
                    )
                self._cond.wait(timeout=remaining)

    def snapshot(self) -> dict[str, Any]:
        self.wait_idle()
        with self._lock:
            c2s = bytes(self._client_to_source)
            s2c = bytes(self._source_to_client)
            connections = self._connections
        return {
            "connections": connections,
            "client_to_source": c2s,
            "source_to_client": s2c,
            "client_to_source_bytes": len(c2s),
            "source_to_client_bytes": len(s2c),
            "total_bytes": len(c2s) + len(s2c),
        }


class _CaptureServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        target_address: tuple[str, int],
        counters: CaptureCounters,
    ) -> None:
        self.target_address = target_address
        self.counters = counters
        super().__init__(server_address, _CaptureHandler)


class _CaptureHandler(socketserver.BaseRequestHandler):
    server: _CaptureServer

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
                    raise TimeoutError("capture proxy relay timed out")
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
                        counters.add_client_to_source(data)
                    else:
                        counters.add_source_to_client(data)
            selector.close()
        finally:
            if upstream is not None:
                try:
                    upstream.close()
                except OSError:
                    pass
            counters.end()


class CaptureProxy:
    def __init__(self, listen_port: int, target_port: int) -> None:
        self.counters = CaptureCounters()
        self._server = _CaptureServer(
            ("127.0.0.1", listen_port),
            ("127.0.0.1", target_port),
            self.counters,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"skeindb-capture-proxy-{listen_port}",
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

    def snapshot(self) -> dict[str, Any]:
        return self.counters.snapshot()


@dataclass
class HttpMessage:
    start_line: str
    header_bytes: int
    transfer_framing_bytes: int
    body: bytes
    wire_body_bytes: int

    @property
    def total_wire_bytes(self) -> int:
        return self.header_bytes + self.wire_body_bytes


def _parse_chunked(stream: bytes, offset: int) -> tuple[bytes, int]:
    start = offset
    body_parts: list[bytes] = []
    while True:
        line_end = stream.find(b"\r\n", offset)
        if line_end < 0:
            raise ValueError("incomplete chunk-size line")
        size_token = stream[offset:line_end].split(b";", 1)[0].strip()
        size = int(size_token, 16)
        offset = line_end + 2
        if size == 0:
            trailer_end = stream.find(b"\r\n\r\n", offset)
            if trailer_end >= 0:
                offset = trailer_end + 4
            else:
                if stream[offset : offset + 2] != b"\r\n":
                    raise ValueError("incomplete final chunk terminator")
                offset += 2
            break
        end = offset + size
        if end + 2 > len(stream):
            raise ValueError("incomplete chunk data")
        body_parts.append(stream[offset:end])
        if stream[end : end + 2] != b"\r\n":
            raise ValueError("missing chunk terminator")
        offset = end + 2
    return b"".join(body_parts), offset - start


def parse_http_stream(stream: bytes) -> list[HttpMessage]:
    messages: list[HttpMessage] = []
    offset = 0
    while offset < len(stream):
        header_end = stream.find(b"\r\n\r\n", offset)
        if header_end < 0:
            raise ValueError(f"unparsed trailing HTTP bytes: {len(stream) - offset}")
        raw_headers = stream[offset : header_end + 4]
        lines = raw_headers[:-4].split(b"\r\n")
        if not lines:
            raise ValueError("missing HTTP start line")
        start_line = lines[0].decode("latin-1")
        headers: dict[str, str] = {}
        for raw_line in lines[1:]:
            if b":" not in raw_line:
                continue
            name, value = raw_line.split(b":", 1)
            headers[name.decode("latin-1").strip().lower()] = (
                value.decode("latin-1").strip()
            )

        body_start = header_end + 4
        transfer_framing = 0
        wire_body_bytes = 0
        if "chunked" in headers.get("transfer-encoding", "").lower():
            body, wire_body_bytes = _parse_chunked(stream, body_start)
            transfer_framing = wire_body_bytes - len(body)
            offset = body_start + wire_body_bytes
        else:
            length = int(headers.get("content-length", "0") or "0")
            end = body_start + length
            if end > len(stream):
                raise ValueError("incomplete content-length body")
            body = stream[body_start:end]
            wire_body_bytes = length
            offset = end

        messages.append(
            HttpMessage(
                start_line=start_line,
                header_bytes=len(raw_headers),
                transfer_framing_bytes=transfer_framing,
                body=body,
                wire_body_bytes=wire_body_bytes,
            )
        )
    return messages


BINARY_FETCH_MAGIC = b"SKOF"
BINARY_FETCH_VERSION = 1


def parse_binary_fetch_body(body: bytes) -> tuple[list[bytes], int]:
    if len(body) < 9 or body[:4] != BINARY_FETCH_MAGIC:
        raise ValueError("invalid binary fetch header")
    if body[4] != BINARY_FETCH_VERSION:
        raise ValueError(f"unsupported binary fetch version {body[4]}")
    count = int.from_bytes(body[5:9], "little")
    offset = 9
    payloads: list[bytes] = []
    framing = 9
    for _ in range(count):
        if offset + 4 > len(body):
            raise ValueError("truncated binary entry length")
        size = int.from_bytes(body[offset : offset + 4], "little")
        offset += 4
        framing += 4
        end = offset + size
        if end > len(body):
            raise ValueError("truncated binary entry payload")
        payloads.append(body[offset:end])
        offset = end
    if offset != len(body):
        raise ValueError("binary fetch body has trailing bytes")
    return payloads, framing


def source_fetch_delta(
    before: dict[str, Any], after: dict[str, Any]
) -> dict[str, int]:
    return {
        "fetch_calls": cas.delta(after, before, "fetch_calls"),
        "fetch_ids_total": cas.delta(after, before, "fetch_ids_total"),
        "objects_served": cas.delta(after, before, "fetch_objects_served"),
        "obj_bytes": cas.delta(after, before, "obj_bytes"),
    }


def decompose(
    request_messages: list[HttpMessage],
    response_messages: list[HttpMessage],
    source_fetch: dict[str, int],
) -> dict[str, Any]:
    if len(request_messages) != len(response_messages):
        raise RuntimeError(
            "HTTP request/response count mismatch: "
            f"{len(request_messages)} != {len(response_messages)}"
        )

    request_headers = sum(m.header_bytes for m in request_messages)
    request_transfer = sum(m.transfer_framing_bytes for m in request_messages)
    request_body = sum(len(m.body) for m in request_messages)
    request_ids_json = 0
    requested_ids = 0

    for message in request_messages:
        payload = json.loads(message.body)
        ids = payload.get("ids")
        if ids is None:
            ids = payload["params"]["ids"]
        requested_ids += len(ids)
        request_ids_json += len(compact_json(ids))

    request_json_other = request_body - request_ids_json
    if request_json_other < 0:
        raise RuntimeError("negative request JSON remainder")

    response_headers = sum(m.header_bytes for m in response_messages)
    response_transfer = sum(m.transfer_framing_bytes for m in response_messages)
    response_body = sum(len(m.body) for m in response_messages)
    response_ids_json = 0
    response_entry_b64_json = 0
    response_binary_framing = 0
    response_binary_transfer = 0
    entry_b64_chars = 0
    decoded_transfer_entry_bytes = 0
    response_objects = 0
    response_modes: set[str] = set()

    for message in response_messages:
        if message.body.startswith(BINARY_FETCH_MAGIC):
            response_modes.add("binary_v1")
            entries, framing = parse_binary_fetch_body(message.body)
            response_binary_framing += framing
            response_binary_transfer += sum(len(entry) for entry in entries)
            decoded_transfer_entry_bytes += sum(len(entry) for entry in entries)
            response_objects += len(entries)
            continue

        response_modes.add("json_rpc")
        payload = json.loads(message.body)
        objects = payload["result"]["objects"]
        response_objects += len(objects)
        for obj in objects:
            object_id = obj["id"]
            entry_b64 = obj["entry_b64"]
            response_ids_json += len(compact_json(object_id))
            response_entry_b64_json += len(compact_json(entry_b64))
            entry_b64_chars += len(entry_b64.encode("ascii"))
            decoded_transfer_entry_bytes += len(base64.b64decode(entry_b64))

    response_json_other = (
        response_body
        - response_ids_json
        - response_entry_b64_json
        - response_binary_framing
        - response_binary_transfer
    )
    if response_json_other < 0:
        raise RuntimeError("negative response remainder")

    wire_total = sum(m.total_wire_bytes for m in request_messages) + sum(
        m.total_wire_bytes for m in response_messages
    )
    categorized_total = (
        request_headers
        + request_transfer
        + request_ids_json
        + request_json_other
        + response_headers
        + response_transfer
        + response_ids_json
        + response_entry_b64_json
        + response_binary_framing
        + response_binary_transfer
        + response_json_other
    )
    if wire_total != categorized_total:
        raise RuntimeError(
            f"wire composition does not close: {wire_total} != {categorized_total}"
        )

    obj_bytes = int(source_fetch["obj_bytes"])
    return {
        "messages": len(request_messages),
        "requested_ids": requested_ids,
        "response_objects": response_objects,
        "wire_total_bytes": wire_total,
        "request": {
            "http_headers_bytes": request_headers,
            "transfer_framing_bytes": request_transfer,
            "ids_json_bytes": request_ids_json,
            "json_rpc_other_bytes": request_json_other,
            "total_bytes": request_headers
            + request_transfer
            + request_ids_json
            + request_json_other,
        },
        "response": {
            "modes": sorted(response_modes),
            "http_headers_bytes": response_headers,
            "transfer_framing_bytes": response_transfer,
            "object_ids_json_bytes": response_ids_json,
            "entry_b64_json_bytes": response_entry_b64_json,
            "binary_protocol_framing_bytes": response_binary_framing,
            "binary_transfer_entry_bytes": response_binary_transfer,
            "json_rpc_other_bytes": response_json_other,
            "total_bytes": response_headers
            + response_transfer
            + response_ids_json
            + response_entry_b64_json
            + response_binary_framing
            + response_binary_transfer
            + response_json_other,
        },
        "derived": {
            "entry_b64_character_bytes": entry_b64_chars,
            "decoded_transfer_entry_bytes": decoded_transfer_entry_bytes,
            "value_store_object_bytes": obj_bytes,
            "base64_text_expansion_bytes": max(
                0, entry_b64_chars - decoded_transfer_entry_bytes
            ),
            "base64_text_expansion_ratio": (
                round(entry_b64_chars / decoded_transfer_entry_bytes, 6)
                if entry_b64_chars and decoded_transfer_entry_bytes
                else 0.0
            ),
            "decoded_transfer_vs_value_store_ratio": (
                round(decoded_transfer_entry_bytes / obj_bytes, 6)
                if obj_bytes
                else 0.0
            ),
            "wire_vs_value_store_ratio": (
                round(wire_total / obj_bytes, 6) if obj_bytes else 0.0
            ),
        },
    }


def preseed_destination(
    destination_url: str,
    dictionary: dict[str, Any],
    value_ids: list[str],
    request_id: int,
) -> int:
    request_id = cas.create_table(
        destination_url, "cas_destination", "seed_values", request_id
    )
    return cas.insert_literal_rows(
        destination_url,
        "cas_destination",
        "seed_values",
        cas.prefill_rows(value_ids, dictionary),
        request_id,
    )


def run_case(
    skeindb_bin: Path,
    scenario: model.Scenario,
    rows_count: int,
    seed: int,
    batch_size: int,
    source_http_port: int,
    destination_http_port: int,
    proxy_port: int,
    source_cluster_port: int,
    destination_cluster_port: int,
) -> dict[str, Any]:
    source_rows = model.build_rows(rows_count, scenario.repetition, seed)

    with tempfile.TemporaryDirectory(prefix="skeindb-wire-composition-") as tmp:
        root = Path(tmp)
        source_proc, source_handle, source_url = cas.launch_node(
            skeindb_bin,
            root / "source-data",
            source_http_port,
            source_cluster_port,
            root / "source.log",
        )
        destination_proc, destination_handle, destination_url = cas.launch_node(
            skeindb_bin,
            root / "destination-data",
            destination_http_port,
            destination_cluster_port,
            root / "destination.log",
        )
        proxy = CaptureProxy(proxy_port, source_http_port)
        proxy.start()

        source_request = 100
        destination_request = 20_000
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
                raise RuntimeError("too few ValueIDs for composition experiment")

            preseed_count = int(round(len(value_ids) * scenario.receiver_overlap))
            preseed_count = max(1, min(len(value_ids) - 1, preseed_count))
            preseed_ids = value_ids[:preseed_count]
            destination_request = preseed_destination(
                destination_url,
                dictionary,
                preseed_ids,
                destination_request,
            )

            preflight, _ = runtime.rpc(
                destination_url,
                "objects.need",
                {"ids": value_ids},
                destination_request,
            )
            destination_request += 1
            missing = preflight["result"].get("missing", [])
            if not missing:
                raise RuntimeError("composition experiment has no missing objects")

            before, _ = runtime.rpc(
                source_url,
                "cluster.replication_stats",
                {},
                source_request,
            )
            source_request += 1

            pull, _ = runtime.rpc(
                destination_url,
                "objects.pull",
                {
                    "source_rpc_url": proxy.url,
                    "ids": value_ids,
                    "batch_size": batch_size,
                },
                destination_request,
            )
            destination_request += 1

            capture = proxy.snapshot()
            after, _ = runtime.rpc(
                source_url,
                "cluster.replication_stats",
                {},
                source_request,
            )
            source_request += 1
            fetch = source_fetch_delta(before["result"], after["result"])

            result = pull["result"]
            if (
                result.get("invalid_ids")
                or result.get("remote_missing")
                or result.get("verification_failed")
            ):
                raise RuntimeError("objects.pull reported transfer integrity failures")
            if result.get("fetched_objects") != len(missing):
                raise RuntimeError(
                    "fetched object count does not match preflight missing set"
                )
            if capture["connections"] != 1:
                raise RuntimeError(
                    f"expected one CR06 keep-alive connection, got {capture['connections']}"
                )

            requests = parse_http_stream(capture["client_to_source"])
            responses = parse_http_stream(capture["source_to_client"])
            breakdown = decompose(requests, responses, fetch)
            if breakdown["wire_total_bytes"] != capture["total_bytes"]:
                raise RuntimeError("HTTP parser total differs from proxy total")

            return {
                "scenario": scenario.name,
                "rows": rows_count,
                "seed": seed,
                "batch_size": batch_size,
                "source_value_ids": len(value_ids),
                "preseeded_value_ids": len(preseed_ids),
                "missing_value_ids": len(missing),
                "pull_batches": result.get("batches"),
                "breakdown": breakdown,
            }
        finally:
            proxy.stop()
            cas.stop_node(
                destination_proc,
                destination_handle,
                destination_url,
                999_998,
            )
            cas.stop_node(source_proc, source_handle, source_url, 999_999)


def pct(part: int, total: int) -> float:
    return round(100.0 * part / total, 3) if total else 0.0


def add_percentages(result: dict[str, Any]) -> None:
    breakdown = result["breakdown"]
    total = breakdown["wire_total_bytes"]
    categories = {
        "request_http_headers": breakdown["request"]["http_headers_bytes"],
        "request_ids_json": breakdown["request"]["ids_json_bytes"],
        "request_json_rpc_other": breakdown["request"]["json_rpc_other_bytes"],
        "request_transfer_framing": breakdown["request"]["transfer_framing_bytes"],
        "response_http_headers": breakdown["response"]["http_headers_bytes"],
        "response_object_ids_json": breakdown["response"]["object_ids_json_bytes"],
        "response_entry_b64_json": breakdown["response"]["entry_b64_json_bytes"],
        "response_binary_protocol_framing": breakdown["response"][
            "binary_protocol_framing_bytes"
        ],
        "response_binary_transfer_entries": breakdown["response"][
            "binary_transfer_entry_bytes"
        ],
        "response_json_rpc_other": breakdown["response"]["json_rpc_other_bytes"],
        "response_transfer_framing": breakdown["response"][
            "transfer_framing_bytes"
        ],
    }
    breakdown["wire_categories"] = {
        key: {"bytes": value, "pct": pct(value, total)}
        for key, value in categories.items()
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# SkeinDB CAS Wire Composition",
        "",
        f"Format: `{report['format']}`  ",
        f"Rows per scenario: **{report['rows']}**  ",
        f"Batch size: **{report['batch_size']}**  ",
        f"Seed: **{report['seed']}**",
        "",
        "The table decomposes the exact HTTP-over-TCP payload bytes used by the CR09 binary-response pull path. The request remains JSON; the response carries canonical transfer entries directly.",
        "",
        "| Scenario | Wire bytes | Request IDs | Request envelope+HTTP | Binary transfer entries | Binary framing | Response envelope+HTTP | Decoded transfer bytes | ValueStore object bytes |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in report["results"]:
        b = result["breakdown"]
        request_fixed = (
            b["request"]["http_headers_bytes"]
            + b["request"]["transfer_framing_bytes"]
            + b["request"]["json_rpc_other_bytes"]
        )
        response_fixed = (
            b["response"]["http_headers_bytes"]
            + b["response"]["transfer_framing_bytes"]
            + b["response"]["json_rpc_other_bytes"]
        )
        lines.append(
            "| {scenario} | {wire} | {req_ids} ({req_pct:.1f}%) | {req_fixed} | "
            "{entry} ({entry_pct:.1f}%) | {binary_framing} | {resp_fixed} | {decoded} | {obj} |".format(
                scenario=result["scenario"],
                wire=b["wire_total_bytes"],
                req_ids=b["request"]["ids_json_bytes"],
                req_pct=pct(b["request"]["ids_json_bytes"], b["wire_total_bytes"]),
                req_fixed=request_fixed,
                entry=b["response"]["binary_transfer_entry_bytes"],
                entry_pct=pct(
                    b["response"]["binary_transfer_entry_bytes"],
                    b["wire_total_bytes"],
                ),
                binary_framing=b["response"]["binary_protocol_framing_bytes"],
                resp_fixed=response_fixed,
                decoded=b["derived"]["decoded_transfer_entry_bytes"],
                obj=b["derived"]["value_store_object_bytes"],
            )
        )

    lines.extend(
        [
            "",
            "## Derived ratios",
            "",
            "| Scenario | Response mode | Decoded transfer / ValueStore object | Total wire / ValueStore object |",
            "|---|---|---:|---:|",
        ]
    )
    for result in report["results"]:
        d = result["breakdown"]["derived"]
        lines.append(
            "| {scenario} | {mode} | {transfer:.3f}x | {wire:.3f}x |".format(
                scenario=result["scenario"],
                mode=",".join(result["breakdown"]["response"]["modes"]),
                transfer=d["decoded_transfer_vs_value_store_ratio"],
                wire=d["wire_vs_value_store_ratio"],
            )
        )

    lines.extend(
        [
            "",
            "Every wire category is derived from captured HTTP bytes and the categories close exactly to the proxy total. Canonical transfer-entry bytes and ValueStore object bytes are derived metrics and are not double-counted.",
            "",
            "The experiment is explanatory byte accounting only; it makes no latency or throughput claim.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skeindb-bin", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=300)
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--base-port", type=int, default=20300)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binary = args.skeindb_bin.resolve()
    if not binary.exists():
        raise SystemExit(f"SkeinDB binary does not exist: {binary}")

    scenarios = (model.DEFAULT_SCENARIOS[0], model.DEFAULT_SCENARIOS[1])
    results: list[dict[str, Any]] = []
    for idx, scenario in enumerate(scenarios):
        base = args.base_port + idx * 20
        result = run_case(
            binary,
            scenario,
            args.rows,
            args.seed + idx * 101,
            args.batch_size,
            source_http_port=base,
            destination_http_port=base + 1,
            proxy_port=base + 2,
            source_cluster_port=base + 100,
            destination_cluster_port=base + 101,
        )
        add_percentages(result)
        results.append(result)
        b = result["breakdown"]
        print(
            "wire composition {scenario}: wire={wire}B ids={ids}B "
            "entry_b64={entry}B decoded_transfer={decoded}B obj={obj}B".format(
                scenario=result["scenario"],
                wire=b["wire_total_bytes"],
                ids=b["request"]["ids_json_bytes"],
                entry=b["response"]["entry_b64_json_bytes"],
                decoded=b["derived"]["decoded_transfer_entry_bytes"],
                obj=b["derived"]["value_store_object_bytes"],
            ),
            flush=True,
        )

    report = {
        "format": FORMAT,
        "kind": "captured_http_wire_composition",
        "rows": args.rows,
        "seed": args.seed,
        "batch_size": args.batch_size,
        "claim_boundary": [
            "Exact plain-HTTP TCP payload bytes are captured and parsed.",
            "Wire categories sum exactly to the proxy byte total.",
            "Decoded transfer-entry and ValueStore object bytes are derived comparisons, not extra wire categories.",
            "No latency or throughput conclusion is drawn.",
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
