import unittest

import cas_batch_sweep as sweep


class CasBatchSweepTests(unittest.TestCase):
    def test_aggregate_uses_32_as_reference(self):
        rows = [
            {
                "scenario": "a",
                "batch_size": 32,
                "zero_overlap": {
                    "http_tcp_payload_bytes": 100,
                    "request_bytes": 20,
                    "batches": 4,
                    "connections": 1,
                },
                "cas_overlap": {
                    "http_tcp_payload_bytes": 50,
                    "request_bytes": 10,
                    "batches": 2,
                    "connections": 1,
                },
            },
            {
                "scenario": "a",
                "batch_size": 64,
                "zero_overlap": {
                    "http_tcp_payload_bytes": 90,
                    "request_bytes": 15,
                    "batches": 2,
                    "connections": 1,
                },
                "cas_overlap": {
                    "http_tcp_payload_bytes": 45,
                    "request_bytes": 8,
                    "batches": 1,
                    "connections": 1,
                },
            },
        ]
        agg = sweep.aggregate(rows)
        self.assertEqual(agg[0]["combined_http_bytes"], 150)
        self.assertEqual(agg[0]["combined_savings_vs_32_pct"], 0.0)
        self.assertEqual(agg[1]["combined_http_bytes"], 135)
        self.assertEqual(agg[1]["combined_savings_vs_32_pct"], 10.0)

    def test_validate_rejects_connection_regression(self):
        rows = [
            {
                "scenario": "a",
                "batch_size": 32,
                "zero_overlap": {
                    "http_tcp_payload_bytes": 100,
                    "connections": 2,
                },
                "cas_overlap": {
                    "http_tcp_payload_bytes": 50,
                    "connections": 1,
                },
            }
        ]
        with self.assertRaises(RuntimeError):
            sweep.validate(rows, [{"combined_http_bytes": 150}])


if __name__ == "__main__":
    unittest.main()
