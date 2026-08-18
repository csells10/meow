import unittest
from unittest.mock import patch

import qa_gamelens_packet4_level2 as preview


class _ScalarQueryParameter:
    def __init__(self, name, type_, value):
        self.name = name
        self.type_ = type_
        self.value = value


class _QueryJobConfig:
    def __init__(self, query_parameters):
        self.query_parameters = query_parameters


class _BigQuery:
    ScalarQueryParameter = _ScalarQueryParameter
    QueryJobConfig = _QueryJobConfig


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return self.rows


class _Client:
    def __init__(self, rows):
        self.rows = list(rows)
        self.queries = []

    def query(self, sql, job_config):
        self.queries.append((sql, job_config))
        return _Result(self.rows.pop(0))


class Packet4Level2PreviewTests(unittest.TestCase):
    def test_load_claims_is_bounded_by_full_identity(self):
        client = _Client([[{"claim_key": "one"}]])
        rows = preview.load_bounded_claim_rows(
            client=client,
            bigquery=_BigQuery,
            project_id="project",
            learning_run_id="cohort",
            capture_id="capture",
            game_id="game",
        )
        self.assertEqual(rows, [{"claim_key": "one"}])
        sql, config = client.queries[0]
        self.assertIn("GameLens_dev.claim_training_examples", sql)
        self.assertIn("learning_run_id = @learning_run_id", sql)
        self.assertIn("capture_id = @capture_id", sql)
        self.assertIn("game_id = @game_id", sql)
        self.assertEqual(
            [parameter.value for parameter in config.query_parameters],
            ["cohort", "capture", "game"],
        )

    @patch.object(preview, "load_accepted_fact_rows")
    @patch.object(preview, "load_bounded_claim_rows")
    @patch.object(preview, "load_final_score_rows")
    @patch.object(preview, "load_canonical_snapshot")
    def test_preview_carries_capture_identity_to_zero_claim_no_op(
        self, snapshot, score_rows, claims, facts
    ):
        snapshot.return_value = {
            "learning_run_id": "cohort",
            "capture_id": "capture",
            "season": 2026,
        }
        score_rows.return_value = [
            {
                "team_type": "away", "awayPts": 17,
                "Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0, "OT": 0,
            },
            {
                "team_type": "home", "homePts": 7,
                "Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0, "OT": 0,
            },
        ]
        claims.return_value = []
        facts.return_value = [{"game_id": "game", "metric": "x"}]

        result = preview.build_packet4_level2_preview(
            client=object(),
            bigquery=object(),
            project_id="project",
            game_id="game",
        )
        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "zero_claims")
        self.assertEqual(result["learning_run_id"], "cohort")
        self.assertEqual(result["capture_id"], "capture")
        self.assertFalse(result["write_performed"])

    def test_cli_requires_explicit_read_only_confirmation(self):
        with self.assertRaisesRegex(ValueError, "--dev-read-only"):
            preview.main(["--game-id", "game"])


if __name__ == "__main__":
    unittest.main()
