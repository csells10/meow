import unittest
from datetime import date

from api_calls.api_utils.controlled_replay_scope import (
    select_games_for_load_date,
)


class ControlledReplayScopeTests(unittest.TestCase):
    def test_requested_date_selects_only_expected_game(self):
        backlog = [
            {"gameID": "20250914_NYG@DAL", "gameDate": "2025-09-14"},
            {"gameID": "20250915_TB@HOU", "gameDate": date(2025, 9, 15)},
        ]

        selected = select_games_for_load_date(
            backlog,
            load_date="2025-09-14",
            controlled_replay=True,
            configured_replay_date="2025-09-14",
        )

        self.assertEqual(
            [game["gameID"] for game in selected],
            ["20250914_NYG@DAL"],
        )

    def test_two_games_on_replay_date_fail_before_processing(self):
        backlog = [
            {"gameID": "20250914_NYG@DAL", "gameDate": "2025-09-14"},
            {"gameID": "20250914_ATL@MIN", "gameDate": "20250914"},
        ]

        with self.assertRaisesRegex(RuntimeError, "exactly one game"):
            select_games_for_load_date(
                backlog,
                load_date="2025-09-14",
                controlled_replay=True,
                configured_replay_date="2025-09-14",
            )

    def test_zero_games_is_an_explicit_empty_selection(self):
        selected = select_games_for_load_date(
            [{"gameID": "20250915_TB@HOU", "gameDate": "2025-09-15"}],
            load_date="2025-09-14",
            controlled_replay=True,
            configured_replay_date="2025-09-14",
        )

        self.assertEqual(selected, [])

    def test_request_must_match_configured_replay_date(self):
        with self.assertRaisesRegex(ValueError, "configured replay date"):
            select_games_for_load_date(
                [],
                load_date="2025-09-15",
                controlled_replay=True,
                configured_replay_date="2025-09-14",
            )

    def test_controlled_replay_requires_request_date(self):
        with self.assertRaisesRegex(ValueError, "load_date"):
            select_games_for_load_date(
                [],
                load_date=None,
                controlled_replay=True,
                configured_replay_date="2025-09-14",
            )


if __name__ == "__main__":
    unittest.main()
