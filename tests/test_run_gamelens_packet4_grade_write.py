from __future__ import annotations

import unittest
from types import SimpleNamespace

import run_gamelens_packet4_grade_write as grade_write
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
        "matchup_lean": {"target_team": ""},
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
        "game_week": "Preseason Week 1",
    }


def _scores():
    return [
        {"team_type": "away", "awayPts": 17, "homePts": 7},
        {"team_type": "home", "awayPts": 17, "homePts": 7},
    ]


class _Client:
    def query(self, query, job_config=None):
        if "pregame_snapshots" in query:
            return _Job([_snapshot()])
        if "Scores.scores" in query:
            return _Job(_scores())
        raise AssertionError(query)


class _Storage:
    plan = {
        "status": "ready",
        "conflict_count": 0,
        "existing_capture_row_count": 0,
        "projected_capture_row_count": 1,
        "expected_inserted_count": 1,
        "expected_unchanged_count": 0,
        "conflicts": [],
        "write_performed": False,
    }

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def plan_grade(self, grade):
        self.grade = grade
        return dict(self.plan)

    def store_grade(self, grade, *, attempt_id):
        self.grade = grade
        self.attempt_id = attempt_id
        return {
            "status": "matched",
            "grades_in": 1,
            "inserted": 1,
            "unchanged": 0,
            "conflicts": 0,
            "grades_out": 1,
            "write_performed": True,
        }


class Packet4GradeWriteTests(unittest.TestCase):
    def test_main_requires_explicit_dev_write_confirmation(self):
        with self.assertRaisesRegex(ValueError, "--confirm-dev-write"):
            grade_write.main(
                [
                    "--game-id",
                    "20260815_DAL@SEA",
                    "--attempt-id",
                    "packet4_test",
                ]
            )

    def test_execute_refuses_production_before_cloud_read(self):
        with self.assertRaisesRegex(ValueError, "only in dev"):
            grade_write.execute_packet4_grade_write(
                client=object(),
                bigquery=_BigQuery,
                runtime_config=SimpleNamespace(
                    is_dev=False,
                    project_id="nfl-stream-406420",
                ),
                game_id="20260815_DAL@SEA",
                attempt_id="packet4_test",
            )

    def test_execute_writes_one_reviewed_grade_and_returns_reconciliation(self):
        result = grade_write.execute_packet4_grade_write(
            client=_Client(),
            bigquery=_BigQuery,
            runtime_config=SimpleNamespace(
                is_dev=True,
                project_id="nfl-stream-406420",
            ),
            game_id="20260815_DAL@SEA",
            attempt_id="packet4_first_write",
            outcome_builder=lambda **_: {
                "result": "No Pick",
                "predicted_team": None,
                "actual_winner": "DAL",
            },
            trust_builder=lambda **_: {"learning_label": "Neutral"},
            storage_factory=_Storage,
        )
        self.assertEqual(result["before_plan"]["existing_capture_row_count"], 0)
        self.assertEqual(result["reconciliation"]["inserted"], 1)
        self.assertEqual(result["reconciliation"]["grades_out"], 1)
        self.assertTrue(result["write_performed"])
        self.assertEqual(result["attempt_id"], "packet4_first_write")

    def test_conflict_stops_before_store(self):
        class ConflictStorage(_Storage):
            plan = {**_Storage.plan, "conflict_count": 1}

            def store_grade(self, grade, *, attempt_id):
                raise AssertionError("store must not run")

        with self.assertRaisesRegex(ValueError, "immutable conflict"):
            grade_write.execute_packet4_grade_write(
                client=_Client(),
                bigquery=_BigQuery,
                runtime_config=SimpleNamespace(
                    is_dev=True,
                    project_id="nfl-stream-406420",
                ),
                game_id="20260815_DAL@SEA",
                attempt_id="packet4_conflict",
                outcome_builder=lambda **_: {"result": "No Pick"},
                trust_builder=lambda **_: {},
                storage_factory=ConflictStorage,
            )


if __name__ == "__main__":
    unittest.main()
