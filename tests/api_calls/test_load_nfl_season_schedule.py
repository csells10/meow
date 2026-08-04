import sys
import types
import unittest
from datetime import date
from unittest.mock import MagicMock, call, patch

import pandas as pd


try:
    from google.cloud import bigquery as _bigquery
except ModuleNotFoundError:
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.Client = MagicMock
    bigquery_module.QueryJobConfig = MagicMock
    bigquery_module.ArrayQueryParameter = MagicMock
    cloud_module = types.ModuleType("google.cloud")
    cloud_module.bigquery = bigquery_module
    google_module = types.ModuleType("google")
    google_module.cloud = cloud_module
    sys.modules.setdefault("google", google_module)
    sys.modules.setdefault("google.cloud", cloud_module)
    sys.modules.setdefault("google.cloud.bigquery", bigquery_module)


from api_calls import load_nfl_season_schedule as schedule


class SeasonBoundaryTests(unittest.TestCase):
    def test_default_range_crosses_into_next_calendar_year(self):
        self.assertEqual(
            schedule.season_date_range(2026),
            (date(2026, 7, 1), date(2027, 2, 28)),
        )

    def test_current_season_changes_in_july(self):
        self.assertEqual(
            schedule.current_nfl_season(date(2027, 1, 15)),
            2026,
        )
        self.assertEqual(
            schedule.current_nfl_season(date(2027, 7, 1)),
            2027,
        )

    def test_filter_uses_nfl_season_not_calendar_year(self):
        games = [
            {
                "gameID": "20260104_NYJ@BUF",
                "gameDate": "20260104",
                "season": "2025",
            },
            {
                "gameID": "20270103_BUF@NYJ",
                "gameDate": "20270103",
                "season": "2026",
            },
        ]

        self.assertEqual(
            schedule.filter_games_for_season(games, 2026),
            [games[1]],
        )


class SeasonScheduleLoaderTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        secret_patcher = patch.object(
            schedule,
            "get_secret",
            return_value="test-key",
        )
        secret_patcher.start()
        self.addCleanup(secret_patcher.stop)

    def test_three_day_dry_run_requests_exact_dates_and_writes_nothing(self):
        def response_for_date(_url, _headers, game_date):
            return [{
                "gameID": f"{game_date}_BUF@NYJ",
                "gameDate": game_date,
                "season": "2026",
            }]

        with patch.object(
            schedule,
            "fetch_schedule_games",
            side_effect=response_for_date,
        ) as fetch, patch.object(
            schedule,
            "transform_schedule_games",
            side_effect=lambda games: pd.DataFrame(games),
        ), patch.object(
            schedule,
            "fetch_existing_game_ids",
            return_value=set(),
        ), patch.object(
            schedule,
            "insert_schedule_rows",
        ) as insert:
            summary = schedule.load_nfl_season_schedule(
                season=2026,
                start_date=date(2026, 7, 30),
                end_date=date(2026, 8, 1),
                write=False,
                sleep_seconds=0,
                client=self.client,
            )

        requested_dates = [
            item.args[2]
            for item in fetch.call_args_list
        ]
        self.assertEqual(
            requested_dates,
            ["20260730", "20260731", "20260801"],
        )
        self.assertEqual(summary["games_to_insert"], 3)
        self.assertEqual(summary["games_inserted"], 0)
        insert.assert_not_called()

    def test_write_inserts_only_missing_game_ids(self):
        games = [
            {
                "gameID": "20260806_DAL@PIT",
                "gameDate": "20260806",
                "season": "2026",
            },
            {
                "gameID": "20260806_BUF@NYJ",
                "gameDate": "20260806",
                "season": "2026",
            },
        ]

        with patch.object(
            schedule,
            "fetch_schedule_games",
            return_value=games,
        ), patch.object(
            schedule,
            "transform_schedule_games",
            return_value=pd.DataFrame(games),
        ), patch.object(
            schedule,
            "fetch_existing_game_ids",
            return_value={"20260806_DAL@PIT"},
        ), patch.object(
            schedule,
            "insert_schedule_rows",
        ) as insert:
            summary = schedule.load_nfl_season_schedule(
                season=2026,
                start_date=date(2026, 8, 6),
                end_date=date(2026, 8, 6),
                write=True,
                sleep_seconds=0,
                client=self.client,
            )

        insert.assert_called_once_with(
            self.client,
            [games[1]],
        )
        self.assertEqual(summary["existing_games_skipped"], 1)
        self.assertEqual(summary["games_inserted"], 1)

    def test_failed_date_does_not_stop_later_dates(self):
        second_game = {
            "gameID": "20260802_BUF@NYJ",
            "gameDate": "20260802",
            "season": "2026",
        }

        with patch.object(
            schedule,
            "fetch_schedule_games",
            side_effect=[RuntimeError("temporary failure"), [second_game]],
        ), patch.object(
            schedule,
            "transform_schedule_games",
            return_value=pd.DataFrame([second_game]),
        ), patch.object(
            schedule,
            "fetch_existing_game_ids",
            return_value=set(),
        ), patch.object(
            schedule,
            "insert_schedule_rows",
        ):
            summary = schedule.load_nfl_season_schedule(
                season=2026,
                start_date=date(2026, 8, 1),
                end_date=date(2026, 8, 2),
                write=False,
                sleep_seconds=0,
                client=self.client,
            )

        self.assertEqual(summary["dates_checked"], 2)
        self.assertEqual(summary["failed_dates"], ["2026-08-01"])
        self.assertEqual(summary["games_to_insert"], 1)

    def test_empty_date_is_not_failure_and_processing_continues(self):
        second_game = {
            "gameID": "20260802_BUF@NYJ",
            "gameDate": "20260802",
            "season": "2026",
        }

        with patch.object(
            schedule,
            "fetch_schedule_games",
            side_effect=[
                ValueError("No valid response for date 20260801"),
                [second_game],
            ],
        ), patch.object(
            schedule,
            "transform_schedule_games",
            return_value=pd.DataFrame([second_game]),
        ), patch.object(
            schedule,
            "fetch_existing_game_ids",
            return_value=set(),
        ), patch.object(
            schedule,
            "insert_schedule_rows",
        ):
            summary = schedule.load_nfl_season_schedule(
                season=2026,
                start_date=date(2026, 8, 1),
                end_date=date(2026, 8, 2),
                write=False,
                sleep_seconds=0,
                client=self.client,
            )

        self.assertEqual(summary["dates_checked"], 2)
        self.assertEqual(summary["failed_dates"], [])
        self.assertEqual(summary["games_to_insert"], 1)


if __name__ == "__main__":
    unittest.main()
