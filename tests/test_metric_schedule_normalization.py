import unittest
from datetime import datetime, timezone

import pandas as pd

from agg import build_metric_facts as facts
from agg import build_windowed_metrics as windows


class TestMetricScheduleNormalization(unittest.TestCase):
    def test_hall_of_fame_weekend_uses_preseason_phase(self):
        result = facts.normalize_game_week(
            "Hall of Fame Weekend",
            "Preseason",
        )

        self.assertEqual(result["season_phase"], "preseason")
        self.assertIsNone(result["phase_week"])
        self.assertTrue(result["is_preseason"])

    def test_future_preseason_label_does_not_require_allow_list(self):
        result = facts.normalize_game_week(
            "NFL Opening Showcase",
            "Preseason",
        )

        self.assertEqual(result["season_phase"], "preseason")
        self.assertIsNone(result["phase_week"])

    def test_numbered_and_postseason_labels_still_parse(self):
        cases = (
            ("Preseason Week 2", "Preseason", "preseason", 2),
            ("Week 7", "Regular Season", "regular_season", 7),
            ("Wild Card", "Postseason", "postseason", 1),
            ("Super Bowl", "Postseason", "postseason", 4),
        )

        for game_week, season_type, expected_phase, expected_week in cases:
            with self.subTest(game_week=game_week):
                result = facts.normalize_game_week(game_week, season_type)
                self.assertEqual(result["season_phase"], expected_phase)
                self.assertEqual(result["phase_week"], expected_week)

    def test_chronology_uses_kickoff_then_game_date_fallback(self):
        source = pd.DataFrame(
            [
                {
                    "game_week": "Hall of Fame Weekend",
                    "season_type": "Preseason",
                    "game_datetime_raw": "2026-08-07 00:00:00 UTC",
                    "game_date": "2026-08-06",
                },
                {
                    "game_week": "NFL Opening Showcase",
                    "season_type": "Preseason",
                    "game_datetime_raw": None,
                    "game_date": "2026-08-08",
                },
            ]
        )

        result = facts.attach_phase_fields(source)

        self.assertEqual(str(result["phase_week"].dtype), "Int64")
        self.assertEqual(str(result["global_week_order"].dtype), "Int64")
        self.assertEqual(
            result["global_week_order"].tolist(),
            [
                int(pd.Timestamp("2026-08-07T00:00:00Z").timestamp()),
                int(pd.Timestamp("2026-08-08T00:00:00Z").timestamp()),
            ],
        )

    def test_unsupported_season_type_fails_fact_validation(self):
        row = {column: "value" for column in facts.OUTPUT_COLUMNS}
        row.update(
            {
                "season": "2026",
                "game_id": "20260806_CAR@ARI",
                "game_date": pd.Timestamp("2026-08-06"),
                "season_type": "Showcase",
                "season_phase": "unknown",
                "phase_week": pd.NA,
                "global_week_order": 1,
                "team_id": "29",
                "team_abv": "CAR",
                "metric": "total_yards",
                "value": 300.0,
                "lens_tags": [],
                "created_at": datetime.now(timezone.utc),
            }
        )

        with self.assertRaisesRegex(ValueError, "unsupported season_type"):
            facts.validate_fact_df(pd.DataFrame([row]), "2026")

    def test_pivot_keeps_unnumbered_game_rows(self):
        source = pd.DataFrame(
            [
                {
                    "season": "2026",
                    "team_id": "1",
                    "team_abv": "ARI",
                    "game_id": "20260806_CAR@ARI",
                    "game_date": "2026-08-06",
                    "game_week": "Hall of Fame Weekend",
                    "season_phase": "preseason",
                    "phase_week": pd.NA,
                    "global_week_order": 1786060800,
                    "metric": "total_yards",
                    "value": 320.0,
                },
                {
                    "season": "2026",
                    "team_id": "29",
                    "team_abv": "CAR",
                    "game_id": "20260806_CAR@ARI",
                    "game_date": "2026-08-06",
                    "game_week": "Hall of Fame Weekend",
                    "season_phase": "preseason",
                    "phase_week": pd.NA,
                    "global_week_order": 1786060800,
                    "metric": "total_yards",
                    "value": 280.0,
                },
            ]
        )

        result = windows.pivot_fact_rows(source)

        self.assertEqual(len(result), 2)
        self.assertEqual(set(result["team_abv"]), {"ARI", "CAR"})
        self.assertEqual(set(result["total_yards"]), {280.0, 320.0})

    def test_pivot_rejects_missing_chronology(self):
        source = pd.DataFrame(
            [
                {
                    "season": "2026",
                    "team_id": "29",
                    "team_abv": "CAR",
                    "game_id": "20260806_CAR@ARI",
                    "game_date": "2026-08-06",
                    "game_week": "Hall of Fame Weekend",
                    "season_phase": "preseason",
                    "phase_week": pd.NA,
                    "global_week_order": pd.NA,
                    "metric": "total_yards",
                    "value": 280.0,
                }
            ]
        )

        with self.assertRaisesRegex(ValueError, "global_week_order"):
            windows.pivot_fact_rows(source)


if __name__ == "__main__":
    unittest.main()
