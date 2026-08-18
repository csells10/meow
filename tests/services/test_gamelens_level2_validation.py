import unittest

from services import gamelens_level2_validation as level2


def _claim(key="claim-1", **overrides):
    row = {
        "learning_run_id": "cohort-1",
        "run_id": "cohort-1",
        "capture_id": "capture-1",
        "game_id": "game-1",
        "claim_key": key,
        "claim_type": "team_comparison_metric",
        "claim_layer": "headline",
    }
    row.update(overrides)
    return row


class _Calculation:
    calls = []

    @classmethod
    def build_actual_index(cls, rows):
        cls.calls.append(("index", rows))
        return {"game-1": {"rows": rows}}

    @classmethod
    def validate_training_row(cls, row, actual_index, abs_tol, pct_tol):
        cls.calls.append(("validate", row["claim_key"], abs_tol, pct_tol))
        result = "unavailable" if row.get("missing") else "validated"
        return {
            "run_id": row["run_id"],
            "claim_key": row["claim_key"],
            "validation_result": result,
            "feature_status": (
                "missing_postgame_actual"
                if result == "unavailable"
                else "validated"
            ),
        }

    @classmethod
    def add_game_level_scores(
        cls, validation_rows, training_rows, valid_threshold, weak_threshold
    ):
        cls.calls.append(("scores", valid_threshold, weak_threshold))
        for row in validation_rows:
            row["qa_read_v2"] = "fixture_score"


class BoundedLevel2ValidationTests(unittest.TestCase):
    def setUp(self):
        _Calculation.calls = []

    def run_worker(self, **overrides):
        kwargs = {
            "learning_run_id": "cohort-1",
            "capture_id": "capture-1",
            "game_id": "game-1",
            "claim_rows": [],
            "actual_fact_rows": [{"game_id": "game-1", "metric": "x"}],
            "facts_accepted": True,
            "calculation_module": _Calculation,
        }
        kwargs.update(overrides)
        return level2.run_bounded_level2_validation(**kwargs)

    def test_waits_for_accepted_facts_before_validation(self):
        result = self.run_worker(
            claim_rows=[_claim()], actual_fact_rows=[], facts_accepted=False
        )
        self.assertEqual(result["status"], "deferred")
        self.assertEqual(result["reason"], "waiting_accepted_facts")
        self.assertEqual(_Calculation.calls, [])
        self.assertFalse(result["write_performed"])

    def test_zero_claims_is_visible_no_op(self):
        result = self.run_worker()
        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "zero_claims")
        self.assertEqual(result["reconciliation"]["claims_in"], 0)
        self.assertEqual(result["reconciliation"]["validations_out"], 0)
        self.assertEqual(_Calculation.calls, [])

    def test_populated_path_reuses_calculation_owner_and_reconciles(self):
        result = self.run_worker(
            claim_rows=[_claim(), _claim("claim-2", missing=True)]
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["reconciliation"]["claims_in"], 2)
        self.assertEqual(result["reconciliation"]["validations_out"], 2)
        self.assertEqual(result["reconciliation"]["unavailable"], 1)
        self.assertEqual(
            result["by_validation_result"],
            {"unavailable": 1, "validated": 1},
        )
        self.assertEqual(
            [row["qa_read_v2"] for row in result["validation_rows"]],
            ["fixture_score", "fixture_score"],
        )
        self.assertEqual(_Calculation.calls[0][0], "index")
        self.assertEqual(_Calculation.calls[-1][0], "scores")

    def test_refuses_cross_capture_claim(self):
        with self.assertRaisesRegex(ValueError, "capture_id disagrees"):
            self.run_worker(claim_rows=[_claim(capture_id="capture-2")])

    def test_refuses_duplicate_claim_key(self):
        with self.assertRaisesRegex(ValueError, "duplicate bounded"):
            self.run_worker(claim_rows=[_claim(), _claim()])

    def test_refuses_cross_game_fact(self):
        with self.assertRaisesRegex(ValueError, "fact game_id disagrees"):
            self.run_worker(
                actual_fact_rows=[{"game_id": "game-2", "metric": "x"}]
            )


if __name__ == "__main__":
    unittest.main()
