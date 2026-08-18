import unittest
from types import SimpleNamespace
from unittest.mock import patch

import run_gamelens_packet4_level2_write as runner


def _preview():
    return {
        "learning_run_id": "cohort-1",
        "capture_id": "capture-1",
        "game_id": "game-1",
        "final_score": {"away": {"total": 17}, "home": {"total": 7}},
        "source_counts": {
            "claims": 0,
            "accepted_fact_rows_total": 130,
            "eligible_actual_fact_rows": 94,
        },
        "status": "no_op",
        "reason": "zero_claims",
        "by_validation_result": {},
        "by_feature_status": {},
        "validation_rows": [],
    }


class _Storage:
    def __init__(self, **_):
        self.plan = {
            "status": "no_op",
            "reason": "zero_claims",
            "conflict_count": 0,
            "rejected_count": 0,
            "expected_updated": 0,
            "expected_unchanged": 0,
            "write_performed": False,
        }

    def plan_validations(self, **_):
        return self.plan

    def store_validations(self, **_):
        return {
            "status": "no_op",
            "reason": "zero_claims",
            "validations_in": 0,
            "selected_claim_rows": 0,
            "updated": 0,
            "unchanged": 0,
            "conflicts": 0,
            "rejected": 0,
            "write_performed": False,
        }


class Packet4Level2WriteTests(unittest.TestCase):
    @patch.object(runner, "build_packet4_level2_preview", return_value=_preview())
    def test_zero_claim_write_attempt_remains_no_op(self, preview):
        result = runner.execute_packet4_level2_write(
            client=object(),
            bigquery=object(),
            runtime_config=SimpleNamespace(is_dev=True, project_id="project"),
            game_id="game-1",
            attempt_id="attempt-1",
            storage_factory=_Storage,
        )
        self.assertEqual(result["validation_status"], "no_op")
        self.assertEqual(result["validation_reason"], "zero_claims")
        self.assertEqual(result["reconciliation"]["updated"], 0)
        self.assertFalse(result["write_performed"])
        preview.assert_called_once()

    def test_refuses_production_before_preview(self):
        with patch.object(
            runner,
            "build_packet4_level2_preview",
            side_effect=AssertionError("preview must not run"),
        ):
            with self.assertRaisesRegex(ValueError, "only in dev"):
                runner.execute_packet4_level2_write(
                    client=object(),
                    bigquery=object(),
                    runtime_config=SimpleNamespace(
                        is_dev=False, project_id="project"
                    ),
                    game_id="game-1",
                    attempt_id="attempt-1",
                )

    def test_conflict_stops_before_store(self):
        class ConflictStorage(_Storage):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.plan = {
                    **self.plan,
                    "status": "conflict",
                    "conflict_count": 1,
                }

            def store_validations(self, **_):
                raise AssertionError("store must not run")

        with patch.object(
            runner, "build_packet4_level2_preview", return_value=_preview()
        ):
            with self.assertRaisesRegex(ValueError, "stored-row conflict"):
                runner.execute_packet4_level2_write(
                    client=object(),
                    bigquery=object(),
                    runtime_config=SimpleNamespace(
                        is_dev=True, project_id="project"
                    ),
                    game_id="game-1",
                    attempt_id="attempt-1",
                    storage_factory=ConflictStorage,
                )

    def test_cli_requires_explicit_confirmation(self):
        with self.assertRaisesRegex(ValueError, "--confirm-dev-write"):
            runner.main(
                ["--game-id", "game-1", "--attempt-id", "attempt-1"]
            )


if __name__ == "__main__":
    unittest.main()
