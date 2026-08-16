import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def _load_game_service():
    query_module = types.ModuleType("queries.game_queries")
    for name in (
        "get_game_header",
        "get_team_metrics",
        "get_final_score",
        "get_team_rankings_for_game",
        "metric_value",
    ):
        setattr(query_module, name, lambda *args, **kwargs: None)
    trust_module = types.ModuleType("services.model_trust_service")
    trust_module.build_model_trust = lambda *args, **kwargs: None
    core_module = types.ModuleType("services.core_area_analysis")
    core_module.build_core_area_comparison = lambda *args, **kwargs: []

    features_module = types.ModuleType("services.claim_language_features")
    features_module.build_runtime_offensive_efficiency_support_by_side = (
        lambda *args, **kwargs: {}
    )
    features_module.build_runtime_two_way_context_by_side = (
        lambda *args, **kwargs: {}
    )
    response_module = types.ModuleType("services.claim_language_response")
    response_module.apply_claim_language_support_to_response_sections = (
        lambda payload, *args, **kwargs: payload
    )
    logging_module = types.ModuleType("utils.logging_setup")
    logging_module.log_event = lambda *args, **kwargs: None

    bigquery_module = types.ModuleType("google.cloud.bigquery")
    cloud_module = types.ModuleType("google.cloud")
    cloud_module.bigquery = bigquery_module
    google_module = types.ModuleType("google")
    google_module.cloud = cloud_module
    stubs = {
        "queries": types.ModuleType("queries"),
        "queries.game_queries": query_module,
        "services.model_trust_service": trust_module,
        "services.core_area_analysis": core_module,
        "services.claim_language_features": features_module,
        "services.claim_language_response": response_module,
        "utils": types.ModuleType("utils"),
        "utils.logging_setup": logging_module,
        "google": google_module,
        "google.cloud": cloud_module,
        "google.cloud.bigquery": bigquery_module,
    }
    path = Path(__file__).parents[1] / "services" / "game_service.py"
    spec = importlib.util.spec_from_file_location(
        "hotfix_game_service_under_test",
        path,
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    return module


game_service = _load_game_service()


HEADER = {
    "away_team": {"abbreviation": "AWY"},
    "home_team": {"abbreviation": "HME"},
    "game_week": "Week 3",
}


def _core_context(core_gap: float) -> dict:
    return {
        "profile_type": "confirmed_edge",
        "core_gap": core_gap,
        "total_core_areas": 5,
    }


class TestGameServiceConfidenceCalibration(unittest.TestCase):
    def test_profile_label_softens_high_without_changing_profile(self):
        result = game_service.build_profile_strength_labels(
            target="AWY",
            confidence="High",
            profile_type="confirmed_edge",
            core_area_context=_core_context(0.44),
            team_edge_context={"edge_strength": "strong"},
            signal_score={"gap": 8},
        )

        self.assertEqual(result["profile_strength"]["label"], "Strong Profile")
        self.assertEqual(result["outcome_confidence"]["label"], "Medium")
        self.assertTrue(result["confidence_calibration"]["confidence_calibrated"])
        self.assertEqual(
            result["confidence_calibration"]["raw_confidence_label"],
            "High",
        )
        self.assertIn("Medium Outcome Confidence", result["display_label"])

    def test_matchup_lean_preserves_pick_and_raw_confidence(self):
        team_comparison = [{"better": "away"} for _ in range(8)]

        with (
            patch.object(
                game_service,
                "build_team_comparison_edge_context",
                return_value={"edge_strength": "strong"},
            ),
            patch.object(
                game_service,
                "build_core_area_context",
                return_value=_core_context(0.44),
            ),
        ):
            result = game_service.build_matchup_lean(
                game_profile=[],
                team_comparison=team_comparison,
                header=HEADER,
                core_area_comparison=[],
            )

        self.assertEqual(result["target_team"], "AWY edge")
        self.assertEqual(result["target_side"], "away")
        self.assertEqual(result["profile_type"], "confirmed_edge")
        self.assertEqual(result["confidence"], "High")
        self.assertEqual(result["raw_signal_confidence"], "High")
        self.assertEqual(result["outcome_confidence"]["label"], "Medium")
        self.assertTrue(result["confidence_calibration"]["confidence_calibrated"])

    def test_week_two_low_cap_remains_in_control(self):
        team_comparison = [{"better": "away"} for _ in range(8)]
        header = {**HEADER, "game_week": "Week 2"}

        with (
            patch.object(
                game_service,
                "build_team_comparison_edge_context",
                return_value={"edge_strength": "strong"},
            ),
            patch.object(
                game_service,
                "build_core_area_context",
                return_value=_core_context(0.44),
            ),
        ):
            result = game_service.build_matchup_lean(
                game_profile=[],
                team_comparison=team_comparison,
                header=header,
                core_area_comparison=[],
            )

        self.assertEqual(result["target_team"], "AWY edge")
        self.assertEqual(result["confidence"], "Low")
        self.assertEqual(result["outcome_confidence"]["label"], "Low")
        self.assertFalse(result["confidence_calibration"]["confidence_calibrated"])
        self.assertIn(
            "early_season_week_1_or_2",
            result["confidence_guardrails"]["reasons"],
        )


if __name__ == "__main__":
    unittest.main()
