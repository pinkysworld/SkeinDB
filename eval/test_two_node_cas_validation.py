import unittest

import two_node_cas_validation as cas


class TwoNodeCasValidationTests(unittest.TestCase):
    def test_prefill_rows_preserve_literal(self):
        dictionary = {
            "a" * 32: {"t": "str", "v": "alpha"},
            "b" * 32: {"t": "str", "v": "beta"},
        }
        rows = cas.prefill_rows(list(dictionary), dictionary)
        self.assertEqual(rows[0]["payload"], dictionary["a" * 32])
        self.assertEqual(rows[1]["payload"], dictionary["b" * 32])
        self.assertEqual(rows[0]["id"], {"t": "i64", "v": 1})

    def test_counter_delta(self):
        before = {"obj_bytes": 100}
        after = {"obj_bytes": 340}
        self.assertEqual(cas.delta(after, before, "obj_bytes"), 240)

    def test_pct(self):
        self.assertEqual(cas.pct(25, 100), 25.0)
        self.assertEqual(cas.pct(0, 0), 0.0)

    def test_validate_result_accepts_idempotent_transfer(self):
        result = {
            "scenario": "test",
            "objects": {
                "source_value_ids": 10,
                "preseeded_value_ids": 4,
                "preflight_missing_ids": 6,
            },
            "first_pull": {
                "already_present": 4,
                "fetched_objects": 6,
                "stored": 6,
                "remote_missing": [],
                "verification_failed": [],
                "invalid_ids": [],
            },
            "postflight_missing_ids": 0,
            "source_fetch": {"objects_served": 6, "obj_bytes": 123},
            "second_pull": {
                "already_present": 10,
                "fetched_objects": 0,
                "stored": 0,
                "batches": 0,
            },
            "second_pull_source_fetch": {
                "fetch_calls": 0,
                "objects_served": 0,
                "obj_bytes": 0,
            },
        }
        cas.validate_scenario_result(result)


if __name__ == "__main__":
    unittest.main()
