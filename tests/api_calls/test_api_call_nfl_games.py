import sys
import types
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd


class FakeArrayQueryParameter:
    def __init__(self, name, parameter_type, values):
        self.name = name
        self.parameter_type = parameter_type
        self.values = values


class FakeQueryJobConfig:
    def __init__(self, query_parameters):
        self.query_parameters = query_parameters


try:
    from google.cloud import bigquery as _bigquery
except ModuleNotFoundError:
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.Client = MagicMock
    bigquery_module.QueryJobConfig = FakeQueryJobConfig
    bigquery_module.ArrayQueryParameter = FakeArrayQueryParameter
    cloud_module = types.ModuleType("google.cloud")
    cloud_module.bigquery = bigquery_module
    google_module = types.ModuleType("google")
    google_module.cloud = cloud_module
    sys.modules.setdefault("google", google_module)
    sys.modules.setdefault("google.cloud", cloud_module)
    sys.modules.setdefault("google.cloud.bigquery", bigquery_module)


helper_module = types.ModuleType("utils.helper")
helper_module.insert_into_bigquery = MagicMock()
helper_module.get_secret = MagicMock(return_value="test-key")
helper_module.fetch_and_validate_api_data = MagicMock()
response_module = types.ModuleType("utils.response_helpers")
response_module.save_raw_response = MagicMock()
logging_module = types.ModuleType("utils.logging_setup")
logging_module.log_event = MagicMock()
utils_module = sys.modules.get("utils") or types.ModuleType("utils")
utils_module.helper = helper_module
utils_module.response_helpers = response_module
utils_module.logging_setup = logging_module
sys.modules["utils"] = utils_module
sys.modules["utils.helper"] = helper_module
sys.modules["utils.response_helpers"] = response_module
sys.modules["utils.logging_setup"] = logging_module


from api_calls import api_call_nfl_games as games


class ScheduleTransformTests(unittest.TestCase):
    def test_transform_builds_exact_schedule_contract(self):
        transformed = games.transform_game_records([{
            "gameID": "20260806_CLE@CHI",
            "gameDate": "20260806",
            "gameTime_epoch": "0",
            "neutralSite": "true",
        }])

        self.assertEqual(transformed.iloc[0]["gameDate"], "2026-08-06")
        self.assertEqual(
            transformed.iloc[0]["gameTime_epoch"],
            "1970-01-01 00:00:00",
        )
        self.assertTrue(transformed.iloc[0]["neutralSite"])
        self.assertFalse(transformed.iloc[0]["boxscore_loaded"])
        self.assertFalse(transformed.iloc[0]["score_loaded"])
        self.assertEqual(len(transformed.columns), 19)

    def test_fetch_games_uses_requested_date(self):
        with patch.object(
            games,
            "fetch_and_validate_api_data",
            return_value={"body": [{"gameID": "game-1"}]},
        ) as fetch:
            result = games.fetch_games_for_date(
                "https://example.test",
                {"header": "value"},
                "20260806",
            )

        self.assertEqual(result, [{"gameID": "game-1"}])
        self.assertEqual(fetch.call_args.args[2], {"gameDate": "20260806"})

    def test_explicit_empty_body_is_valid_no_game_response(self):
        with patch.object(
            games,
            "fetch_and_validate_api_data",
            return_value={"body": []},
        ) as fetch:
            result = games.fetch_games_for_date(
                "https://example.test",
                {"header": "value"},
                "20260804",
            )

        self.assertEqual(result, [])
        fetch.assert_called_once_with(
            "https://example.test",
            {"header": "value"},
            {"gameDate": "20260804"},
            allow_empty_body=True,
        )

    def test_missing_response_body_is_rejected(self):
        with patch.object(
            games,
            "fetch_and_validate_api_data",
            return_value={},
        ):
            with self.assertRaisesRegex(ValueError, "No valid response"):
                games.fetch_games_for_date("url", {}, "20260806")


class ScheduleDateProcessingTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.query.return_value.result.return_value = None

    def test_empty_response_deletes_and_inserts_nothing(self):
        with (
            patch.object(games, "fetch_games_for_date", return_value=[]),
            patch.object(games, "save_raw_response"),
            patch.object(games, "bq", self.client),
            patch.object(games, "insert_into_bigquery") as insert,
        ):
            result = games.process_game_date(
                "project.League.schedule",
                "url",
                {},
                "20260806",
            )

        self.assertEqual(result["status"], "no_op")
        self.assertEqual(result["inserted_row_count"], 0)
        self.client.query.assert_not_called()
        insert.assert_not_called()

    def test_success_uses_parameterized_delete_then_inserts(self):
        frame = pd.DataFrame([
            {"gameID": "game-1"},
            {"gameID": "game-2"},
        ])
        with (
            patch.object(games, "fetch_games_for_date", return_value=[{}]),
            patch.object(games, "save_raw_response"),
            patch.object(games, "transform_game_records", return_value=frame),
            patch.object(games, "bq", self.client),
            patch.object(games, "insert_into_bigquery") as insert,
        ):
            result = games.process_game_date(
                "project.League.schedule",
                "url",
                {},
                "20260806",
            )

        sql = self.client.query.call_args.args[0]
        job_config = self.client.query.call_args.kwargs["job_config"]
        self.assertIn("DELETE FROM `project.League.schedule`", sql)
        self.assertIn("UNNEST(@game_ids)", sql)
        self.assertEqual(job_config.query_parameters[0].name, "game_ids")
        self.assertEqual(
            job_config.query_parameters[0].values,
            ["game-1", "game-2"],
        )
        insert.assert_called_once_with(
            "project.League.schedule",
            [{"gameID": "game-1"}, {"gameID": "game-2"}],
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["inserted_row_count"], 2)

    def test_delete_or_insert_failure_is_not_hidden(self):
        frame = pd.DataFrame([{"gameID": "game-1"}])
        self.client.query.side_effect = RuntimeError("delete failed")
        with (
            patch.object(games, "fetch_games_for_date", return_value=[{}]),
            patch.object(games, "save_raw_response"),
            patch.object(games, "transform_game_records", return_value=frame),
            patch.object(games, "bq", self.client),
        ):
            with self.assertRaisesRegex(RuntimeError, "delete failed"):
                games.process_game_date(
                    "project.League.schedule",
                    "url",
                    {},
                    "20260806",
                )

    def test_insert_failure_is_not_hidden(self):
        frame = pd.DataFrame([{"gameID": "game-1"}])
        with (
            patch.object(games, "fetch_games_for_date", return_value=[{}]),
            patch.object(games, "save_raw_response"),
            patch.object(games, "transform_game_records", return_value=frame),
            patch.object(games, "bq", self.client),
            patch.object(
                games,
                "insert_into_bigquery",
                side_effect=RuntimeError("insert failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "insert failed"):
                games.process_game_date(
                    "project.League.schedule",
                    "url",
                    {},
                    "20260806",
                )


class FetchNflGamesTests(unittest.TestCase):
    @staticmethod
    def _date_result(game_date, *, status="no_op"):
        if status == "success":
            return {
                "status": "success",
                "game_date": game_date,
                "selected_game_ids": [f"{game_date}_BUF@NYJ"],
                "inserted_row_count": 1,
            }
        return {
            "status": "no_op",
            "game_date": game_date,
            "selected_game_ids": [],
            "inserted_row_count": 0,
        }

    def test_controlled_replay_skips_schedule_before_secret_or_api(self):
        replay_config = MagicMock(is_controlled_replay=True)
        with (
            patch.object(games, "RUNTIME_CONFIG", replay_config),
            patch.object(games, "get_secret") as secret,
            patch.object(games, "process_yesterday_games") as yesterday,
            patch.object(games, "process_game_date") as process_date,
        ):
            summary = games.fetch_nfl_games(load_date="2025-09-14")

        self.assertEqual(summary["status"], "no_op")
        self.assertEqual(
            summary["no_op_reason"],
            "controlled_replay_schedule_skipped",
        )
        secret.assert_not_called()
        yesterday.assert_not_called()
        process_date.assert_not_called()

    def test_daily_run_requests_yesterday_today_and_next_two_days(self):
        fixed_now = datetime(2026, 8, 1, 8, 0, 0)
        daily_config = MagicMock(is_controlled_replay=False)
        clock = MagicMock()
        clock.now.return_value = fixed_now
        with (
            patch.object(games, "RUNTIME_CONFIG", daily_config),
            patch.object(games, "datetime", clock),
            patch.object(
                games,
                "process_yesterday_games",
                side_effect=lambda *args: self._date_result(
                    args[3],
                    status="success",
                ),
            ) as yesterday,
            patch.object(
                games,
                "process_game_date",
                side_effect=lambda *args: self._date_result(args[3]),
            ) as process_date,
        ):
            summary = games.fetch_nfl_games()

        self.assertEqual(
            summary["requested_dates"],
            ["20260731", "20260801", "20260802", "20260803"],
        )
        self.assertEqual(yesterday.call_args.args[3], "20260731")
        self.assertEqual(
            [item.args[3] for item in process_date.call_args_list],
            ["20260801", "20260802", "20260803"],
        )
        self.assertEqual(summary["status"], "success")
        self.assertEqual(summary["inserted_row_count"], 1)

    def test_historical_daily_request_is_an_explicit_no_op(self):
        daily_config = MagicMock(is_controlled_replay=False)
        with (
            patch.object(games, "RUNTIME_CONFIG", daily_config),
            patch.object(games, "get_secret") as secret,
        ):
            summary = games.fetch_nfl_games(load_date="2025-09-14")

        self.assertEqual(summary["status"], "no_op")
        self.assertEqual(
            summary["no_op_reason"],
            "historical_schedule_skipped",
        )
        secret.assert_not_called()

    def test_date_failure_is_reported_in_summary(self):
        fixed_now = datetime(2026, 8, 1, 8, 0, 0)
        daily_config = MagicMock(is_controlled_replay=False)
        clock = MagicMock()
        clock.now.return_value = fixed_now
        with (
            patch.object(games, "RUNTIME_CONFIG", daily_config),
            patch.object(games, "datetime", clock),
            patch.object(
                games,
                "process_yesterday_games",
                side_effect=RuntimeError("schedule delete failed"),
            ),
            patch.object(
                games,
                "process_game_date",
                side_effect=lambda *args: self._date_result(args[3]),
            ),
        ):
            summary = games.fetch_nfl_games()

        self.assertEqual(summary["status"], "partial_failure")
        self.assertEqual(summary["failed_date_count"], 1)
        self.assertEqual(
            summary["failures"],
            [{
                "game_date": "20260731",
                "error": "schedule delete failed",
            }],
        )


if __name__ == "__main__":
    unittest.main()
