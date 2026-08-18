import unittest
from types import SimpleNamespace
from unittest.mock import patch

import run_gamelens_packet4_level3_write as runner


def _preview(**overrides):
    result = {
        "learning_run_id": "cohort-1",
        "capture_id": "capture-1",
        "game_id": "game-1",
        "final_score": {"away": {"total": 17}, "home": {"total": 7}},
        "facts_counts": {
            "accepted_fact_rows_total": 130,
            "eligible_actual_fact_rows": 94,
            "claims": 0,
        },
        "level2_gate": {
            "status": "no_op",
            "reason": "zero_claims",
            "reconciliation": {
                "claims_in": 0,
                "validations_out": 0,
                "write_performed": False,
            },
        },
        "status": "no_op",
        "reason": "zero_claims",
        "formula_version": None,
        "source_counts": {"claims": 0, "level2_unavailable": 0},
        "feature_rows": [],
        "reconciliation": {
            "claims_in": 0,
            "features_out": 0,
            "rejected": 0,
            "postgame_fields_admitted": 0,
            "write_performed": False,
        },
    }
    result.update(overrides)
    return result


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

    def plan_features(self, **_):
        return self.plan

    def store_features(self, **_):
        return {
            "status": "no_op",
            "reason": "zero_claims",
            "features_in": 0,
            "selected_claim_rows": 0,
            "updated": 0,
            "unchanged": 0,
            "conflicts": 0,
            "rejected": 0,
            "write_performed": False,
        }


class Packet4Level3WriteTests(unittest.TestCase):
    @patch.object(runner, "build_packet4_level3_preview", return_value=_preview())
    def test_zero_claim_write_attempt_remains_no_op(self, preview):
        result = runner.execute_packet4_level3_write(
            client=object(),
            bigquery=object(),
            runtime_config=SimpleNamespace(is_dev=True, project_id="project"),
            game_id="game-1",
            attempt_id="attempt-1",
            storage_factory=_Storage,
        )
        self.assertEqual(result["feature_status"], "no_op")
        self.assertEqual(result["feature_reason"], "zero_claims")
        self.assertEqual(result["facts_counts"]["accepted_fact_rows_total"], 130)
        self.assertEqual(result["reconciliation"]["updated"], 0)
        self.assertFalse(result["write_performed"])
        preview.assert_called_once()

    def test_refuses_production_before_preview(self):
        with patch.object(
            runner,
            "build_packet4_level3_preview",
            side_effect=AssertionError("preview must not run"),
        ):
            with self.assertRaisesRegex(ValueError, "only in dev"):
                runner.execute_packet4_level3_write(
                    client=object(),
                    bigquery=object(),
                    runtime_config=SimpleNamespace(
                        is_dev=False, project_id="project"
                    ),
                    game_id="game-1",
                    attempt_id="attempt-1",
                )

    def test_blocked_level2_stops_before_storage(self):
        blocked = _preview(status="blocked", reason="level2_failed")
        with patch.object(
            runner, "build_packet4_level3_preview", return_value=blocked
        ):
            with self.assertRaisesRegex(ValueError, "blocked by the Level 2 gate"):
                runner.execute_packet4_level3_write(
                    client=object(),
                    bigquery=object(),
                    runtime_config=SimpleNamespace(
                        is_dev=True, project_id="project"
                    ),
                    game_id="game-1",
                    attempt_id="attempt-1",
                    storage_factory=lambda **_: (_ for _ in ()).throw(
                        AssertionError("storage must not be constructed")
                    ),
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

            def store_features(self, **_):
                raise AssertionError("store must not run")

        with patch.object(
            runner, "build_packet4_level3_preview", return_value=_preview()
        ):
            with self.assertRaisesRegex(ValueError, "stored-row conflict"):
                runner.execute_packet4_level3_write(
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
