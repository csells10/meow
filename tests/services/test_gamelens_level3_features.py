import unittest

from services import gamelens_level3_features as level3


def _claim(key="claim-1", **overrides):
    row = {
        "learning_run_id": "cohort-1",
        "capture_id": "capture-1",
        "run_id": "cohort-1",
        "game_id": "game-1",
        "claim_key": key,
        "season": "2026",
        "claimed_team": "DAL",
        "claimed_side": "away",
        "opponent_team": "SEA",
        "opponent_side": "home",
        "claim_type": "team_comparison_metric",
        "claim_layer": "headline",
        "claim_name": "Yards",
        "core_area": "Offensive Output",
        "category": "Offensive Rhythm",
        "metric": "yards_per_play",
        "pregame_raw_gap": 1.2,
        "pregame_percentile_gap": 10.0,
        "pregame_abs_percentile_gap": 10.0,
        "claimed_team_value": 6.0,
        "opponent_team_value": 4.8,
        "core_area_agreement_rate": 0.75,
        "feature_status": "validated",
        "feature_notes": "level2 audit",
        "feature_formula_version": None,
        "validation_result": "validated",
        "actual_team": "DAL",
        "actual_gap": 100.0,
        "actual_winner": "DAL",
        "final_margin_abs": 10,
        "qa_read_v2": "good_reasoning_correct_outcome",
    }
    row.update(overrides)
    return row


class _Calculation:
    DEFAULT_FORMULA_VERSION = "fixture_formula_v1"
    received = None

    @classmethod
    def build_feature_updates(cls, training_rows, formula_version):
        cls.received = training_rows
        for row in training_rows:
            leaked = level3.POSTGAME_TARGET_FIELDS.intersection(row)
            if leaked:
                raise AssertionError(f"postgame leakage: {sorted(leaked)}")
        return [
            {
                "run_id": row["run_id"],
                "claim_key": row["claim_key"],
                "game_id": row["game_id"],
                "feature_formula_version": formula_version,
                "offense_finish_score": 0.4,
            }
            for row in training_rows
        ]

    @staticmethod
    def build_summary(rows):
        return {"feature_rows": len(rows)}


class BoundedLevel3FeatureTests(unittest.TestCase):
    def setUp(self):
        _Calculation.received = None

    def run_worker(self, **overrides):
        kwargs = {
            "learning_run_id": "cohort-1",
            "capture_id": "capture-1",
            "game_id": "game-1",
            "claim_rows": [],
            "level2_status": "no_op",
            "calculation_module": _Calculation,
        }
        kwargs.update(overrides)
        return level3.run_bounded_level3_features(**kwargs)

    def test_zero_claims_is_visible_no_op(self):
        result = self.run_worker()
        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["reason"], "zero_claims")
        self.assertEqual(result["reconciliation"]["features_out"], 0)
        self.assertIsNone(_Calculation.received)

    def test_level2_failure_blocks_level3_without_calculation(self):
        result = self.run_worker(
            claim_rows=[_claim()], level2_status="failed"
        )
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "level2_failed")
        self.assertIsNone(_Calculation.received)

    def test_populated_path_strips_every_postgame_target(self):
        result = self.run_worker(
            claim_rows=[_claim()], level2_status="completed"
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["formula_version"], "fixture_formula_v1")
        self.assertEqual(result["reconciliation"]["claims_in"], 1)
        self.assertEqual(result["reconciliation"]["features_out"], 1)
        self.assertEqual(
            result["reconciliation"]["postgame_fields_admitted"], 0
        )
        received = _Calculation.received[0]
        self.assertEqual(set(received), set(level3.LEVEL3_CALCULATION_INPUT_FIELDS))
        self.assertFalse(level3.POSTGAME_TARGET_FIELDS.intersection(received))

    def test_unavailable_level2_row_may_proceed(self):
        result = self.run_worker(
            claim_rows=[_claim(validation_result="unavailable")],
            level2_status="completed",
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["source_counts"]["level2_unavailable"], 1)

    def test_populated_claim_requires_validation_state(self):
        with self.assertRaisesRegex(ValueError, "missing Level 2"):
            self.run_worker(
                claim_rows=[_claim(validation_result=None)],
                level2_status="completed",
            )

    def test_refuses_cross_capture_claim(self):
        with self.assertRaisesRegex(ValueError, "capture_id disagrees"):
            self.run_worker(
                claim_rows=[_claim(capture_id="capture-2")],
                level2_status="completed",
            )


if __name__ == "__main__":
    unittest.main()
