import re
import unittest

from analytics.metric_registry import (
    EXPECTED_METRICS,
    METRIC_REGISTRY,
    REQUIRED_FIELDS,
    validate_metric_registry,
)


APPROVED_TAGS = {
    "blocked_fg": [
        "special-teams",
        "blocked-kicks",
        "field-goal-defense",
        "disruption",
        "rare-event",
        "volatility",
    ],
    "blocked_punt": [
        "special-teams",
        "blocked-kicks",
        "punt-pressure",
        "field-position",
        "disruption",
        "rare-event",
        "volatility",
    ],
    "blocked_xp": [
        "special-teams",
        "blocked-kicks",
        "extra-point-defense",
        "disruption",
        "rare-event",
        "volatility",
    ],
    "defensive_tds": [
        "defense",
        "defensive-scoring",
        "takeaways",
        "swing-play",
        "rare-event",
        "volatility",
    ],
    "penalty_count": [
        "penalties",
        "discipline",
        "drive-killers",
        "team-wide",
        "supporting",
    ],
    "penalty_yards": [
        "penalties",
        "discipline",
        "field-position",
        "drive-killers",
        "team-wide",
        "supporting",
    ],
    "safeties": [
        "defense",
        "defensive-scoring",
        "safeties",
        "field-position",
        "rare-event",
        "volatility",
    ],
    "turnovers": [
        "turnovers",
        "giveaways",
        "ball-security",
        "risk",
        "supporting",
    ],
    "passing_first_downs": [
        "drive-sustainability",
        "passing-production",
        "offensive-output",
        "volume-sensitive",
    ],
    "rushing_first_downs": [
        "drive-sustainability",
        "rushing-production",
        "offensive-output",
        "volume-sensitive",
    ],
    "first_downs_from_penalties": [
        "drive-sustainability",
        "penalties",
        "opponent-penalties",
        "offensive-context",
    ],
    "two_point_conversions": [
        "scoring",
        "two-point-conversions",
        "situational",
        "rare-event",
        "volatility",
    ],
    "defensive_two_point_returns": [
        "defense",
        "defensive-scoring",
        "two-point-return",
        "swing-play",
        "rare-event",
        "volatility",
    ],
    "defensive_or_special_teams_tds": [
        "defense",
        "special-teams",
        "non-offensive-scoring",
        "combined-source-metric",
        "overlap-risk",
        "context",
    ],
}

APPROVED_LABELS = {
    "blocked_fg": "Blocked Field Goals",
    "blocked_punt": "Blocked Punts",
    "blocked_xp": "Blocked Extra Points",
    "defensive_tds": "Defensive Touchdowns",
    "penalty_count": "Accepted Penalties Committed",
    "penalty_yards": "Accepted Penalty Yards",
    "safeties": "Defensive Safeties",
    "turnovers": "Turnovers Committed",
    "passing_first_downs": "Passing First Downs",
    "rushing_first_downs": "Rushing First Downs",
    "first_downs_from_penalties": "First Downs From Penalties",
    "two_point_conversions": "Two-Point Conversions",
    "defensive_two_point_returns": "Defensive Two-Point Conversion Returns",
    "defensive_or_special_teams_tds": "Defensive or Special Teams Touchdowns",
}

CONTEXT_ONLY_METRICS = {
    "first_downs_from_penalties",
    "defensive_or_special_teams_tds",
}


class MetricRegistryHotfixTests(unittest.TestCase):
    def test_registry_has_exact_approved_metric_coverage(self):
        validate_metric_registry()

        approved = set(APPROVED_TAGS)
        self.assertEqual(approved, set(APPROVED_LABELS))
        self.assertTrue(approved.issubset(EXPECTED_METRICS))
        self.assertTrue(approved.issubset(METRIC_REGISTRY))

    def test_approved_metrics_have_complete_shared_metadata(self):
        for metric in APPROVED_TAGS:
            with self.subTest(metric=metric):
                config = METRIC_REGISTRY[metric]
                self.assertEqual(set(config), REQUIRED_FIELDS)
                self.assertEqual(config["raw_or_derived"], "raw")
                self.assertEqual(config["aggregation_method"], "sum")
                self.assertIsNone(config["numerator"])
                self.assertIsNone(config["denominator"])
                self.assertEqual(config["format"], "integer")
                self.assertEqual(config["decimals"], 0)
                self.assertFalse(config["include_in_core_area_advantage"])
                self.assertFalse(config["confidence_eligible"])
                self.assertTrue(config["definition"].strip())
                self.assertTrue(config["notes"].strip())

    def test_labels_and_lens_tags_match_approved_contract(self):
        tag_pattern = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")

        for metric, expected_tags in APPROVED_TAGS.items():
            with self.subTest(metric=metric):
                config = METRIC_REGISTRY[metric]
                self.assertEqual(config["label"], APPROVED_LABELS[metric])
                self.assertEqual(config["lens_tags"], expected_tags)
                self.assertEqual(len(expected_tags), len(set(expected_tags)))
                self.assertTrue(all(tag_pattern.fullmatch(tag) for tag in expected_tags))

    def test_penalties_use_existing_offensive_output_pillar(self):
        for metric in ("penalty_count", "penalty_yards"):
            with self.subTest(metric=metric):
                config = METRIC_REGISTRY[metric]
                self.assertEqual(config["category"], "Team Discipline")
                self.assertEqual(config["core_area"], "Offensive Output")
                self.assertEqual(config["comparison_direction"], "lower")
                self.assertFalse(config["higher_is_better"])

    def test_context_only_and_supporting_controls_are_explicit(self):
        for metric in APPROVED_TAGS:
            with self.subTest(metric=metric):
                config = METRIC_REGISTRY[metric]
                if metric in CONTEXT_ONLY_METRICS:
                    self.assertEqual(config["ranking_usage"], "context_only")
                    self.assertEqual(config["signal_strength"], "context")
                    self.assertFalse(config["edge_language_allowed"])
                else:
                    self.assertEqual(config["ranking_usage"], "edge")
                    self.assertEqual(config["signal_strength"], "supporting")
                    self.assertTrue(config["edge_language_allowed"])

        self.assertEqual(
            METRIC_REGISTRY["first_downs_from_penalties"]["comparison_direction"],
            "context",
        )
        self.assertEqual(
            METRIC_REGISTRY["defensive_or_special_teams_tds"][
                "data_quality_status"
            ],
            "watch",
        )

        for metric in set(APPROVED_TAGS) - {"defensive_or_special_teams_tds"}:
            self.assertEqual(METRIC_REGISTRY[metric]["data_quality_status"], "good")


if __name__ == "__main__":
    unittest.main()
