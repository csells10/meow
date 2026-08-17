from __future__ import annotations

import unittest

import qa_gamelens_packet4_dry_grade as dry_grade
from services.gamelens_learning_contract import payload_sha256


class _Config:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _Parameter:
    def __init__(self, *args):
        self.args = args


class _BigQuery:
    QueryJobConfig = _Config
    ScalarQueryParameter = _Parameter


class _Job:
    def __init__(self, rows):
        self.rows = rows

    def result(self):
        return self.rows


def _snapshot():
    payload = {
        "header": {
            "game_id": "20260815_DAL@SEA",
            "away_team": {"abbreviation": "DAL"},
            "home_team": {"abbreviation": "SEA"},
        },
        "game_profile": [],
        "team_comparison": [],
        "matchup_lean": {"target_team": "DAL edge"},
        "final_score": None,
        "model_outcome": None,
    }
    return {
        "learning_run_id": "gamelens_2026_preseason_v1",
        "capture_id": "capture_dal_sea",
        "game_id": "20260815_DAL@SEA",
        "environment": "dev",
        "capture_status": "captured",
        "payload_sha256": payload_sha256(payload),
        "response_payload": payload,
        "metric_pipeline_run_id": "metric_20260816",
        "season": "2026",
        "season_type": "Preseason",
        "game_week": "Preseason 2",
    }


def _score_rows():
    return [
        {
            "team_type": "away",
            "Q1": 7,
            "Q2": 7,
            "Q3": 3,
            "Q4": 7,
            "OT": 0,
            "awayPts": 24,
            "homePts": 17,
        },
        {
            "team_type": "home",
            "Q1": 3,
            "Q2": 7,
            "Q3": 0,
            "Q4": 7,
            "OT": 0,
            "awayPts": 24,
            "homePts": 17,
        },
    ]


class _Client:
    def __init__(self):
        self.queries = []

    def query(self, query, job_config=None):
        self.queries.append((query, job_config))
        if "pregame_snapshots" in query:
            return _Job([_snapshot()])
        if "Scores.scores" in query:
            return _Job(_score_rows())
        raise AssertionError(query)


class _Storage:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def plan_grade(self, grade):
        return {
            "status": "ready",
            "expected_inserted_count": 1,
            "expected_unchanged_count": 0,
            "conflict_count": 0,
            "conflicts": [],
            "existing_capture_row_count": 0,
            "projected_capture_row_count": 1,
            "target_table": (
                "nfl-stream-406420.GameLens_dev.game_model_outcomes"
            ),
            "merge_key": ["learning_run_id", "capture_id"],
            "write_performed": False,
        }


class Packet4DryGradeTests(unittest.TestCase):
    def test_main_requires_explicit_read_only_confirmation(self):
        with self.assertRaisesRegex(ValueError, "--dev-read-only"):
            dry_grade.main(["--game-id", "20260815_DAL@SEA"])

    def test_final_score_requires_exactly_one_row_per_team(self):
        score = dry_grade.build_final_score(_score_rows())
        self.assertEqual(score["away"]["total"], 24)
        self.assertEqual(score["home"]["total"], 17)
        with self.assertRaisesRegex(ValueError, "exactly two"):
            dry_grade.build_final_score(_score_rows()[:1])
        with self.assertRaisesRegex(ValueError, "one away and one home"):
            dry_grade.build_final_score([_score_rows()[0], _score_rows()[0]])

    def test_dry_plan_reuses_frozen_grade_and_projects_without_write(self):
        client = _Client()
        result = dry_grade.build_packet4_dry_grade_plan(
            client=client,
            bigquery=_BigQuery,
            project_id="nfl-stream-406420",
            game_id="20260815_DAL@SEA",
            outcome_builder=lambda **_: {
                "result": "Correct",
                "predicted_team": "DAL",
                "actual_winner": "DAL",
            },
            trust_builder=lambda **_: {"learning_label": "Aligned"},
            storage_factory=_Storage,
        )
        self.assertEqual(result["source_counts"]["canonical_snapshot_rows"], 1)
        self.assertEqual(result["source_counts"]["final_score_rows"], 2)
        self.assertEqual(result["storage_plan"]["existing_capture_row_count"], 0)
        self.assertEqual(result["storage_plan"]["projected_capture_row_count"], 1)
        self.assertFalse(result["write_performed"])
        self.assertEqual(result["grade"]["model_outcome"]["result"], "Correct")
        for query, config in client.queries:
            dry_grade.assert_read_only_sql(query)
            self.assertEqual(config.query_parameters[0].args[2], "20260815_DAL@SEA")


if __name__ == "__main__":
    unittest.main()
