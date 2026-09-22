import unittest
import runtime_redundancy_validation as runtime


class RuntimeRedundancyValidationTests(unittest.TestCase):
    def test_typed_row_and_predicate(self):
        row = runtime.typed_row({"id": 7, "payload": "x", "revision": 2})
        self.assertEqual(row["id"], {"t": "i64", "v": 7})
        self.assertEqual(row["payload"], {"t": "str", "v": "x"})
        predicate = runtime.id_predicate(7)
        self.assertEqual(predicate["b"]["lit"]["v"], 7)

    def test_fake_missing_ids_are_fixed_width_and_distinct(self):
        real = ["a" * 32, "b" * 32]
        fake = runtime.fake_missing_ids(real)
        self.assertEqual(len(fake), 2)
        self.assertTrue(all(len(value) == 32 for value in fake))
        self.assertTrue(set(fake).isdisjoint(real))

    def test_pct(self):
        self.assertEqual(runtime.pct(25, 100), 25.0)
        self.assertEqual(runtime.pct(0, 0), 0.0)

    def test_query_ast_selects_primary_key(self):
        q = runtime.query_ast("db", "items")
        projection = q["body"]["select"]["projection"]
        self.assertEqual(projection[0]["expr"]["col"], "id")


if __name__ == "__main__":
    unittest.main()
