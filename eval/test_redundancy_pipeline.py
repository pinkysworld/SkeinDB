import unittest
import redundancy_pipeline as rp


class RedundancyPipelineTests(unittest.TestCase):
    def test_default_report_is_deterministic(self):
        self.assertEqual(rp.run_benchmark(rows=120, seed=7), rp.run_benchmark(rows=120, seed=7))

    def test_replication_overlap_monotonically_reduces_transfer(self):
        rows = rp.build_rows(120, repetition=0.5, seed=2)
        low = rp.measure_replication(rows, 0.2)
        high = rp.measure_replication(rows, 0.8)
        self.assertGreater(high["saved_bytes"], low["saved_bytes"])
        self.assertLess(high["cas_transfer_bytes"], low["cas_transfer_bytes"])

    def test_high_repetition_improves_storage_savings(self):
        low = rp.measure_storage(rp.build_rows(240, repetition=0.1, seed=3))
        high = rp.measure_storage(rp.build_rows(240, repetition=0.9, seed=3))
        self.assertGreater(high["savings_pct"], low["savings_pct"])

    def test_small_change_patch_is_smaller_than_full_result(self):
        base = rp.build_rows(200, repetition=0.6, seed=4)
        current = rp.mutate_rows(base, change_rate=0.02, seed=5)
        result = rp.measure_query_delivery(base, current)
        self.assertLess(result["patch_response_bytes"], result["full_response_bytes"])
        self.assertEqual(result["selected_delivery_mode"], "patch")

    def test_lifecycle_accounting_matches_components(self):
        result = rp.run_scenario(rp.DEFAULT_SCENARIOS[1], rows=100, seed=9)
        expected_baseline = (
            result["storage"]["inline_bytes"]
            + result["replication"]["naive_object_bytes"]
            + result["query_delivery"]["full_response_bytes"]
        )
        expected_optimized = (
            result["storage"]["adaptive_ref_bytes"]
            + result["replication"]["cas_transfer_bytes"]
            + result["query_delivery"]["selected_delivery_bytes"]
        )
        self.assertEqual(result["lifecycle_byte_work"]["baseline_bytes"], expected_baseline)
        self.assertEqual(result["lifecycle_byte_work"]["redundancy_aware_bytes"], expected_optimized)


if __name__ == "__main__":
    unittest.main()
