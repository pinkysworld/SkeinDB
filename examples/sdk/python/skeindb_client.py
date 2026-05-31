"""Minimal, dependency-free SkeinQL client for SkeinDB.

This is a reference driver that talks to the SkeinQL JSON-RPC surface
(`POST /api/v1/rpc`) using only the Python standard library. It is intentionally
small so it doubles as documentation: read it top-to-bottom to understand the
request/response envelope, error handling, and the cacheable prepared-query GET.

The envelope helpers (`build_request` / `parse_response`) are pure functions and
the HTTP transport is injectable, so the behavior is fully unit-testable without
a running server (see ``test_skeindb_client.py``).

Quick start (requires a running ``skeindb`` on localhost:8080)::

    from skeindb_client import SkeinClient

    client = SkeinClient("http://localhost:8080")
    caps = client.capabilities()
    rows = client.select("SELECT 1 AS one")
    print(rows)
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

SKEINQL_VERSION = "1.0"


class SkeinError(Exception):
    """Raised when the SkeinQL envelope reports ``ok: false``."""

    def __init__(self, code: str, message: str, details: Optional[dict] = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.details = details or {}


def build_request(method: str, params: Optional[dict] = None, request_id: str = "req-1") -> dict:
    """Construct a SkeinQL request envelope.

    Every request carries the protocol version, an opaque ``id`` echoed back by
    the server, the ``method`` name, and a ``params`` object.
    """
    return {
        "skeinql": SKEINQL_VERSION,
        "id": request_id,
        "method": method,
        "params": params or {},
    }


def parse_response(envelope: dict) -> Any:
    """Return ``result`` from a SkeinQL response or raise :class:`SkeinError`.

    HTTP 200 may still contain an RPC error, so callers must always inspect the
    envelope ``ok`` flag rather than the HTTP status alone.
    """
    if not isinstance(envelope, dict):
        raise SkeinError("invalid_response", "response was not a JSON object")
    if envelope.get("ok"):
        return envelope.get("result")
    error = envelope.get("error") or {}
    raise SkeinError(
        code=str(error.get("code", "unknown")),
        message=str(error.get("message", "request failed")),
        details=error.get("details") if isinstance(error.get("details"), dict) else None,
    )


# A transport takes the request envelope (already JSON-serialisable) and returns
# the decoded response envelope. Swapping this out makes the client testable.
Transport = Callable[[dict], dict]


@dataclass
class SkeinClient:
    base_url: str = "http://localhost:8080"
    token: Optional[str] = None
    timeout: float = 10.0
    transport: Optional[Transport] = None
    _request_seq: int = field(default=0, init=False, repr=False)

    def _next_id(self) -> str:
        self._request_seq += 1
        return f"req-{self._request_seq}"

    def _http_transport(self, envelope: dict) -> dict:
        url = f"{self.base_url.rstrip('/')}/api/v1/rpc"
        body = json.dumps(envelope).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:  # surface server-side errors cleanly
            payload = exc.read().decode("utf-8", "replace")
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                raise SkeinError("http_error", f"HTTP {exc.code}: {payload}") from exc

    def rpc(self, method: str, params: Optional[dict] = None) -> Any:
        """Invoke a SkeinQL method and return its ``result``."""
        envelope = build_request(method, params, request_id=self._next_id())
        transport = self.transport or self._http_transport
        return parse_response(transport(envelope))

    # ----- Convenience helpers for common method families -------------------

    def capabilities(self) -> Any:
        """Probe advertised features before relying on experimental families."""
        return self.rpc("system.capabilities")

    def select(self, query: str, params: Optional[dict] = None) -> Any:
        """Run a SkeinQL SELECT and return the result payload."""
        call_params: Dict[str, Any] = {"query": query}
        if params:
            call_params["params"] = params
        return self.rpc("query.select", call_params)

    def exec_sql(self, sql: str) -> Any:
        """Execute MySQL/PostgreSQL-style SQL through the compatibility translator."""
        return self.rpc("sql.exec", {"sql": sql})


def main() -> None:  # pragma: no cover - manual smoke entry point
    client = SkeinClient()
    print("capabilities:", json.dumps(client.capabilities(), indent=2)[:400])
    print("select:", client.select("SELECT 1 AS one"))


if __name__ == "__main__":  # pragma: no cover
    main()
