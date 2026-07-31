import copy
import sys
import types
import unittest
from contextlib import ExitStack
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch


try:
    from google.cloud import bigquery as _bigquery
except ModuleNotFoundError:
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.Client = MagicMock
    bigquery_module.QueryJobConfig = MagicMock
    bigquery_module.ScalarQueryParameter = MagicMock
    cloud_module = types.ModuleType("google.cloud")
    cloud_module.bigquery = bigquery_module
    google_module = types.ModuleType("google")
    google_module.cloud = cloud_module
    sys.modules.setdefault("google", google_module)
    sys.modules.setdefault("google.cloud", cloud_module)
    sys.modules.setdefault("google.cloud.bigquery", bigquery_module)


REPO_ROOT = Path(__file__).resolve().parents[2]
if not (REPO_ROOT / "utils" / "helper.py").exists():
    helper_module = types.ModuleType("utils.helper")
    helper_module.get_secret = MagicMock(return_value="test-key")
    helper_module.fetch_and_validate_api_data = MagicMock()
    response_module = types.ModuleType("utils.response_helpers")
    response_module.save_raw_response = MagicMock()
    logging_module = types.ModuleType("utils.logging_setup")
    logging_module.log_event = MagicMock()
    utils_module = types.ModuleType("utils")
    utils_module.helper = helper_module
    utils_module.response_helpers = response_module
    utils_module.logging_setup = logging_module
    sys.modules.setdefault("utils", utils_module)
    sys.modules.setdefault("utils.helper", helper_module)
    sys.modules.setdefault("utils.response_helpers", response_module)
    sys.modules.setdefault("utils.logging_setup", logging_module)

if (REPO_ROOT / "utils" / "helper.py").exists():
    with patch("utils.helper.get_secret", return_value="test-key"):
        from api_calls import api_call_nfl_scores as scores
else:
    from api_calls import api_call_nfl_scores as scores


GAME_ID = "20250907_MIA@IND"
GAME_DATA = {
    "gameID": GAME_ID,
    "gameStatus": "Completed",
    "gameStatusCode": "Final",
    "period": "Final",
    "gameTime_epoch": "1757275200",
    "teamIDHome": "13",
    "teamIDAway": "20",
    "homePts": "20",
    "awayPts": "14",
    "lineScore": {
        "home": {
            "teamAbv": "IND",
            "Q1": "7",
            "Q2": "3",
            "Q3": "3",
            "Q4": "7",
            "OT": "0",
            "totalPts": "20",
        },
        "away": {
            "teamAbv": "MIA",
            "Q1": "0",
            "Q2": "7",
            "Q3": "0",
            "Q4": "7",
            "OT": "0",
            "totalPts": "14",
        },
    },
}


class BuildScoreRowsTests(unittest.TestCase):
    def test_non_final_game_is_rejected(self):
        game_data = copy.deepcopy(GAME_DATA)
        game_data.update({
            "gameStatus": "In Progress",
            "gameStatusCode": "2",
            "period": "3",
        })

        with self.assertRaisesRegex(ValueError, "not final"):
            scores.build_score_rows(GAME_ID, game_data)

    def test_empty_home_and_away_line_scores_are_rejected(self):
        game_data = copy.deepcopy(GAME_DATA)
        game_data["lineScore"] = {"home": {}, "away": {}}

        with self.assertRaisesRegex(ValueError, "home lineScore"):
            scores.build_score_rows(GAME_ID, game_data)

    def test_missing_team_is_rejected(self):
        game_data = copy.deepcopy(GAME_DATA)
        game_data["teamIDAway"] = None

        with self.assertRaisesRegex(ValueError, "away team identity"):
            scores.build_score_rows(GAME_ID, game_data)

    def test_valid_final_score_builds_exact_table_contract(self):
        rows = scores.build_score_rows(GAME_ID, GAME_DATA)

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {row["team_type"] for row in rows},
            {"home", "away"},
        )
        self.assertTrue(
            all(set(row) == set(scores.SCORE_COLUMNS) for row in rows)
        )
        self.assertEqual(
            rows[0]["homePts"],
            20,
        )
        self.assertEqual(
            rows[0]["awayPts"],
            14,
        )

    def test_legitimate_zero_scores_are_accepted(self):
        game_data = copy.deepcopy(GAME_DATA)
        game_data["awayPts"] = "0"
        game_data["lineScore"]["away"].update({
            "Q1": "0",
            "Q2": "0",
            "Q3": "0",
            "Q4": "0",
            "OT": "0",
            "totalPts": "0",
        })

        rows = scores.build_score_rows(GAME_ID, game_data)

        self.assertEqual(rows[1]["awayPts"], 0)


class ReconcileGameRowsTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.rows = scores.build_score_rows(GAME_ID, GAME_DATA)

    def test_same_game_retry_is_a_verified_no_op(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                scores,
                "fetch_stored_game_rows",
                return_value=copy.deepcopy(self.rows),
            ))
            insert = stack.enter_context(
                patch.object(scores, "insert_rows_bq")
            )
            delete = stack.enter_context(
                patch.object(scores, "delete_game_rows")
            )
            inserted = scores.reconcile_game_rows(
                self.client,
                GAME_ID,
                self.rows,
            )

        self.assertFalse(inserted)
        insert.assert_not_called()
        delete.assert_not_called()

    def test_valid_correction_replaces_only_target_game(self):
        old_rows = copy.deepcopy(self.rows)
        old_rows[0]["Q4"] = 4
        old_rows[0]["homePts"] = 17
        old_rows[1]["homePts"] = 17

        with ExitStack() as stack:
            stack.enter_context(patch.object(
                scores,
                "fetch_stored_game_rows",
                side_effect=[old_rows, copy.deepcopy(self.rows)],
            ))
            insert = stack.enter_context(
                patch.object(scores, "insert_rows_bq")
            )
            delete = stack.enter_context(
                patch.object(scores, "delete_game_rows")
            )
            inserted = scores.reconcile_game_rows(
                self.client,
                GAME_ID,
                self.rows,
            )

        self.assertTrue(inserted)
        delete.assert_called_once_with(self.client, GAME_ID)
        insert.assert_called_once_with(self.client, self.rows)

    def test_bigquery_date_objects_match_expected_strings(self):
        stored_rows = copy.deepcopy(self.rows)
        for row in stored_rows:
            row["game_date_est"] = date.fromisoformat(
                row["game_date_est"]
            )
            row["game_datetime_est"] = datetime.strptime(
                row["game_datetime_est"],
                "%Y-%m-%d %H:%M:%S",
            )

        self.assertTrue(
            scores.rows_are_complete_and_equal(
                stored_rows,
                self.rows,
            )
        )

    def test_marker_insert_errors_raise(self):
        self.client.insert_rows_json.return_value = [{"reason": "test"}]

        with patch.object(scores, "log_event"):
            with self.assertRaises(RuntimeError):
                scores.mark_score_as_loaded(self.client, GAME_ID)


class FetchNflScoresTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.second_game_id = "20250907_CAR@JAX"
        self.second_game = copy.deepcopy(GAME_DATA)
        self.second_game.update({
            "gameID": self.second_game_id,
            "teamIDHome": "30",
            "teamIDAway": "29",
        })
        self.backlog = [
            {"gameID": GAME_ID, "gameDate": "2025-09-07"},
            {
                "gameID": self.second_game_id,
                "gameDate": date(2025, 9, 7),
            },
            {
                "gameID": "20250908_MIN@CHI",
                "gameDate": "2025-09-08",
            },
        ]
        self.patches = [
            patch.object(
                scores.bigquery,
                "Client",
                return_value=self.client,
            ),
            patch.object(
                scores,
                "fetch_scores_to_process",
                return_value=self.backlog,
            ),
            patch.object(scores, "save_raw_response"),
            patch.object(scores, "log_event"),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_date_endpoint_is_called_once_for_same_day_backlog(self):
        api_response = {
            "body": {
                GAME_ID: GAME_DATA,
                self.second_game_id: self.second_game,
            }
        }
        with ExitStack() as stack:
            fetch = stack.enter_context(patch.object(
                scores,
                "fetch_and_validate_api_data",
                return_value=api_response,
            ))
            reconcile = stack.enter_context(patch.object(
                scores,
                "reconcile_game_rows",
                return_value=True,
            ))
            mark = stack.enter_context(
                patch.object(scores, "mark_score_as_loaded")
            )
            result = scores.fetch_nfl_scores(
                load_date="2025-09-07"
            )

        self.assertEqual(result, 2)
        fetch.assert_called_once()
        querystring = fetch.call_args.args[2]
        self.assertEqual(querystring["gameDate"], "20250907")
        self.assertNotIn("gameID", querystring)
        self.assertEqual(reconcile.call_count, 2)
        self.assertEqual(mark.call_count, 2)

    def test_invalid_score_writes_no_rows_and_no_marker(self):
        game_data = copy.deepcopy(GAME_DATA)
        game_data["lineScore"] = {"home": {}, "away": {}}
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                scores,
                "fetch_and_validate_api_data",
                return_value={"body": {GAME_ID: game_data}},
            ))
            reconcile = stack.enter_context(
                patch.object(scores, "reconcile_game_rows")
            )
            mark = stack.enter_context(
                patch.object(scores, "mark_score_as_loaded")
            )
            result = scores.fetch_nfl_scores(
                load_date="20250907"
            )

        self.assertEqual(result, 0)
        reconcile.assert_not_called()
        mark.assert_not_called()

    def test_marker_failure_counts_no_success(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                scores,
                "fetch_scores_to_process",
                return_value=[
                    {"gameID": GAME_ID, "gameDate": "20250907"}
                ],
            ))
            stack.enter_context(patch.object(
                scores,
                "fetch_and_validate_api_data",
                return_value={"body": {GAME_ID: GAME_DATA}},
            ))
            stack.enter_context(patch.object(
                scores,
                "reconcile_game_rows",
                return_value=True,
            ))
            stack.enter_context(patch.object(
                scores,
                "mark_score_as_loaded",
                side_effect=RuntimeError("marker failed"),
            ))
            result = scores.fetch_nfl_scores()

        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()
