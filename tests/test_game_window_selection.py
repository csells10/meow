import unittest
from unittest.mock import patch


with patch("google.cloud.bigquery.Client"):
    from queries import game_queries


class TestGameWindowSelection(unittest.TestCase):
    def test_canonical_season_type_owns_window_selection(self):
        cases = (
            (
                {
                    "season_type": "Preseason",
                    "game_week": "Hall of Fame Weekend",
                },
                "preseason_to_date",
            ),
            (
                {
                    "season_type": "Preseason",
                    "game_week": "NFL Opening Showcase",
                },
                "preseason_to_date",
            ),
            (
                {
                    "season_type": "Regular Season",
                    "game_week": "Preseason Week 1",
                },
                "regular_season_to_date",
            ),
            (
                {
                    "season_type": "Postseason",
                    "game_week": "Wild Card",
                },
                "regular_season_to_date",
            ),
            (
                {
                    "season_type": "Postseason",
                    "game_week": "Divisional Showcase",
                },
                "regular_plus_postseason_to_date",
            ),
        )

        for header, expected in cases:
            with self.subTest(header=header):
                self.assertEqual(
                    game_queries.select_window_type(header),
                    expected,
                )

    def test_missing_season_type_keeps_legacy_fallback(self):
        cases = (
            (
                {"game_week": "Preseason Week 2"},
                "preseason_to_date",
            ),
            (
                {"game_week": "Divisional Round"},
                "regular_plus_postseason_to_date",
            ),
            (
                {"game_week": "Week 8"},
                "regular_season_to_date",
            ),
        )

        for header, expected in cases:
            with self.subTest(header=header):
                self.assertEqual(
                    game_queries.select_window_type(header),
                    expected,
                )


if __name__ == "__main__":
    unittest.main()
