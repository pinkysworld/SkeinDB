import unittest

import two_node_http_wire_validation as wire


class WireCountersTests(unittest.TestCase):
    def test_counter_snapshot_and_reset(self):
        counters = wire.WireCounters()
        counters.begin()
        counters.add_client_to_source(100)
        counters.add_source_to_client(250)
        counters.end()
        snapshot = counters.snapshot()
        self.assertEqual(snapshot["connections"], 1)
        self.assertEqual(snapshot["client_to_source_bytes"], 100)
        self.assertEqual(snapshot["source_to_client_bytes"], 250)
        self.assertEqual(snapshot["total_http_tcp_payload_bytes"], 350)
        counters.reset()
        self.assertEqual(counters.snapshot()["total_http_tcp_payload_bytes"], 0)

    def test_transfer_metrics(self):
        fetch = {"obj_bytes": 1000}
        raw = {
            "connections": 2,
            "active_connections": 0,
            "client_to_source_bytes": 300,
            "source_to_client_bytes": 1700,
            "total_http_tcp_payload_bytes": 2000,
        }
        measured = wire.transfer_metrics(fetch, raw)
        self.assertEqual(measured["wire_minus_materialized_value_bytes"], 1000)
        self.assertEqual(measured["total_wire_vs_materialized_value_ratio"], 2.0)
        self.assertEqual(measured["materialized_value_bytes_per_wire_byte"], 0.5)

    def test_source_fetch_delta(self):
        before = {
            "fetch_calls": 2,
            "fetch_ids_total": 10,
            "fetch_objects_served": 8,
            "obj_bytes": 500,
        }
        after = {
            "fetch_calls": 4,
            "fetch_ids_total": 18,
            "fetch_objects_served": 16,
            "obj_bytes": 1300,
        }
        delta = wire.source_fetch_delta(before, after)
        self.assertEqual(delta["fetch_calls"], 2)
        self.assertEqual(delta["objects_served"], 8)
        self.assertEqual(delta["obj_bytes"], 800)


if __name__ == "__main__":
    unittest.main()
