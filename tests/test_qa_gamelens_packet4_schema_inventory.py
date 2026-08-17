from __future__ import annotations

import unittest

import qa_gamelens_packet4_schema_inventory as inventory


class _Field:
    def __init__(self, name, field_type="STRING", mode="NULLABLE"):
        self.name = name
        self.field_type = field_type
        self.mode = mode


class _Table:
    def __init__(self):
        self.num_rows = 4
        self.schema = [
            _Field("game_id", mode="REQUIRED"),
            _Field("learning_run_id"),
            _Field("capture_id"),
        ]


class _QueryJob:
    def result(self):
        return [{"game_id": "20260815_DAL@SEA", "row_count": 1}]


class _Client:
    def __init__(self):
        self.queries = []

    def get_table(self, table_id):
        self.table_id = table_id
        return _Table()

    def query(self, query, job_config=None):
        self.queries.append((query, job_config))
        return _QueryJob()


class _BigQuery:
    class QueryJobConfig:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class ArrayQueryParameter:
        def __init__(self, *args):
            self.args = args


class Packet4InventoryTests(unittest.TestCase):
    def test_table_inventory_is_bounded_to_approved_sources(self):
        self.assertEqual(
            [
                (dataset, table, required)
                for dataset, table, _, required
                in inventory.table_specs_for_season("2026")
            ],
            [
                ("GameLens_dev", "pregame_snapshots", True),
                ("Scores", "scores", True),
                ("Analytics", "game_model_outcomes", True),
                ("Analytics", "game_model_trust_details", True),
                ("GameLens_dev", "claim_training_examples", True),
                ("GameLens_dev", "stage_runs", True),
                ("GameLens_dev", "stage_game_results", True),
                ("GameLens_dev", "game_model_outcomes", False),
                ("Analytics", "game_team_metric_facts_2026", True),
            ],
        )

    def test_game_count_query_is_parameterized_and_read_only(self):
        query = inventory.build_game_count_query(
            "nfl-stream-406420.GameLens_dev.claim_training_examples",
            {"game_id", "capture_id", "learning_run_id"},
        )
        self.assertIn("game_id IN UNNEST(@game_ids)", query)
        self.assertIn("capture_id_populated_count", query)
        inventory.assert_read_only_sql(query)

    def test_mutating_sql_is_rejected(self):
        for keyword in ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP"):
            with self.subTest(keyword=keyword):
                with self.assertRaises(ValueError):
                    inventory.assert_read_only_sql(f"{keyword} table_name")

    def test_inspect_table_reports_schema_lineage_and_selected_game_counts(self):
        client = _Client()
        result = inventory.inspect_table(
            client=client,
            bigquery=_BigQuery,
            table_id="nfl-stream-406420.GameLens_dev.claim_training_examples",
            role="packet3_claim_rows",
            required=True,
            game_ids=["20260815_DAL@SEA"],
        )
        self.assertEqual(result["status"], "available")
        self.assertEqual(result["row_count"], 4)
        self.assertEqual(result["field_count"], 3)
        self.assertEqual(
            result["lineage_fields_present"],
            ["learning_run_id", "capture_id", "game_id"],
        )
        self.assertEqual(result["selected_game_counts"][0]["row_count"], 1)
        self.assertEqual(len(client.queries), 1)

    def test_unavailable_table_is_visible_not_fatal(self):
        class MissingClient:
            def get_table(self, table_id):
                raise RuntimeError("not found")

        result = inventory.inspect_table(
            client=MissingClient(),
            bigquery=_BigQuery,
            table_id="project.dataset.missing",
            role="missing",
            required=True,
            game_ids=[],
        )
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["error_type"], "RuntimeError")


if __name__ == "__main__":
    unittest.main()
