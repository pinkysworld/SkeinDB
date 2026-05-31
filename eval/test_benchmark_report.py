"""Network-free unit tests for eval/benchmark_report.py.

Run from the repo root::

    python -m unittest eval.test_benchmark_report

or::

    python -m unittest discover -s eval -p 'test_*.py'
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from benchmark_report import (  # noqa: E402
    format_ns,
    render_markdown,
    summarize_report,
)

SAMPLE_REPORT = {
    "sql": "SELECT 1",
    "concurrency": 8,
    "requests_per_transport": 1000,
    "results": [
        {
            "transport": "quic",
            "request_shape": "sql.exec",
            "protocol_version": "h3",
            "samples": 1000,
            "latency": {
                "min_ns": 90_000,
                "p50_ns": 120_000,
                "p95_ns": 240_000,
                "p99_ns": 300_000,
                "max_ns": 800_000,
                "mean_ns": 150_000.0,
            },
        },
        {
            "transport": "http2",
            "request_shape": "sql.exec",
            "protocol_version": "h2",
            "samples": 1000,
            "latency": {
                "min_ns": 100_000,
                "p50_ns": 180_000,
                "p95_ns": 320_000,
                "p99_ns": 410_000,
                "max_ns": 950_000,
                "mean_ns": 210_000.0,
            },
        },
    ],
}


class FormatNsTests(unittest.TestCase):
    def test_units(self):
        self.assertEqual(format_ns(500), "500 ns")
        self.assertEqual(format_ns(1_500), "1.50 us")
        self.assertEqual(format_ns(2_000_000), "2.00 ms")
        self.assertEqual(format_ns(3_000_000_000), "3.000 s")


class SummarizeTests(unittest.TestCase):
    def test_rows_are_sorted_by_transport(self):
        summary = summarize_report(SAMPLE_REPORT)
        self.assertEqual([r["transport"] for r in summary["rows"]], ["http2", "quic"])

    def test_fastest_transport_by_p50(self):
        summary = summarize_report(SAMPLE_REPORT)
        self.assertEqual(summary["fastest_transport_by_p50"], "quic")

    def test_workload_metadata_passthrough(self):
        summary = summarize_report(SAMPLE_REPORT)
        self.assertEqual(summary["sql"], "SELECT 1")
        self.assertEqual(summary["concurrency"], 8)
        self.assertEqual(summary["requests_per_transport"], 1000)

    def test_empty_report_has_no_fastest(self):
        summary = summarize_report({"results": []})
        self.assertIsNone(summary["fastest_transport_by_p50"])
        self.assertEqual(summary["rows"], [])


class RenderTests(unittest.TestCase):
    def test_markdown_is_deterministic_and_contains_key_sections(self):
        env = {
            "git_commit": "abc1234",
            "git_dirty": "clean",
            "rustc": "rustc 1.80.0",
            "os": "TestOS 1.0",
            "machine": "x86_64",
            "cpu_count": "8",
            "python": "3.12.0",
        }
        summary = summarize_report(SAMPLE_REPORT)
        first = render_markdown(env, summary)
        second = render_markdown(env, summary)
        self.assertEqual(first, second)  # deterministic
        self.assertIn("# SkeinDB Transport Benchmark Report", first)
        self.assertIn("| git_commit | abc1234 |", first)
        self.assertIn("Fastest transport by p50: **quic**", first)
        self.assertIn("| http2 | h2 |", first)
        self.assertIn("| quic | h3 |", first)
        # quic p50 of 120_000 ns should render as microseconds.
        self.assertIn("120.00 us", first)


if __name__ == "__main__":
    unittest.main()
