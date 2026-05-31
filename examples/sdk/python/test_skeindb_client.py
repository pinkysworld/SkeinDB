"""Network-free unit tests for the reference SkeinQL Python client.

Run from the repo root::

    python -m unittest discover examples/sdk/python

These tests inject a fake transport so they exercise the envelope contract and
error handling without a running SkeinDB server, which keeps them CI-friendly.
"""

import unittest

from skeindb_client import (
    SKEINQL_VERSION,
    SkeinClient,
    SkeinError,
    build_request,
    parse_response,
)


class BuildRequestTests(unittest.TestCase):
    def test_build_request_shape(self):
        env = build_request("query.select", {"query": "SELECT 1"}, request_id="req-7")
        self.assertEqual(env["skeinql"], SKEINQL_VERSION)
        self.assertEqual(env["id"], "req-7")
        self.assertEqual(env["method"], "query.select")
        self.assertEqual(env["params"], {"query": "SELECT 1"})

    def test_build_request_defaults_params_to_empty_object(self):
        env = build_request("system.capabilities")
        self.assertEqual(env["params"], {})


class ParseResponseTests(unittest.TestCase):
    def test_parse_response_returns_result_on_ok(self):
        result = parse_response({"id": "req-1", "ok": True, "result": {"rows": []}})
        self.assertEqual(result, {"rows": []})

    def test_parse_response_raises_on_error_envelope(self):
        with self.assertRaises(SkeinError) as ctx:
            parse_response(
                {
                    "id": "req-1",
                    "ok": False,
                    "error": {
                        "code": "invalid_request",
                        "message": "Missing field: query",
                        "details": {"field": "query"},
                    },
                }
            )
        self.assertEqual(ctx.exception.code, "invalid_request")
        self.assertEqual(ctx.exception.details, {"field": "query"})

    def test_parse_response_rejects_non_object(self):
        with self.assertRaises(SkeinError):
            parse_response(["not", "an", "object"])


class SkeinClientTests(unittest.TestCase):
    def test_rpc_uses_injected_transport_and_increments_id(self):
        seen = []

        def fake_transport(envelope):
            seen.append(envelope)
            return {"id": envelope["id"], "ok": True, "result": {"echo": envelope["method"]}}

        client = SkeinClient(transport=fake_transport)
        self.assertEqual(client.rpc("a.method"), {"echo": "a.method"})
        self.assertEqual(client.rpc("b.method"), {"echo": "b.method"})
        self.assertEqual([e["id"] for e in seen], ["req-1", "req-2"])

    def test_select_builds_query_params(self):
        captured = {}

        def fake_transport(envelope):
            captured.update(envelope)
            return {"id": envelope["id"], "ok": True, "result": {"columns": ["one"]}}

        client = SkeinClient(transport=fake_transport)
        client.select("SELECT 1 AS one", params={"limit": 10})
        self.assertEqual(captured["method"], "query.select")
        self.assertEqual(captured["params"], {"query": "SELECT 1 AS one", "params": {"limit": 10}})

    def test_error_envelope_propagates_as_exception(self):
        def fake_transport(envelope):
            return {"id": envelope["id"], "ok": False, "error": {"code": "not_found", "message": "x"}}

        client = SkeinClient(transport=fake_transport)
        with self.assertRaises(SkeinError) as ctx:
            client.capabilities()
        self.assertEqual(ctx.exception.code, "not_found")


if __name__ == "__main__":
    unittest.main()
