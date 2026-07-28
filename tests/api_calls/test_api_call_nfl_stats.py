import sys
import types
import unittest
from contextlib import ExitStack
from dataclasses import dataclass
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

if not (REPO_ROOT / "api_calls" / "api_utils" / "parse_nfl_stats.py").exists():
    parse_module = types.ModuleType("api_calls.api_utils.parse_nfl_stats")
    parse_module.parse_game_stats = MagicMock()
    sys.modules.setdefault("api_calls.api_utils.parse_nfl_stats", parse_module)

if not (
    REPO_ROOT / "api_calls" / "api_utils" / "validate_nfl_boxscore.py"
).exists():
    validator_module = types.ModuleType(
        "api_calls.api_utils.validate_nfl_boxscore"
    )
    validator_module.validate_nfl_boxscore = MagicMock()
    sys.modules.setdefault(
        "api_calls.api_utils.validate_nfl_boxscore",
        validator_module,
    )

if (REPO_ROOT / "utils" / "helper.py").exists():
    with patch("utils.helper.get_secret", return_value="test-key"):
        from api_calls import api_call_nfl_stats as stats
else:
    from api_calls import api_call_nfl_stats as stats


@dataclass
class Validation:
    accepted: bool
    code: str = "accepted"
    reason: str = "accepted"
    team_ids: tuple = ("12", "2")


class FetchNflStatsTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.game_id = "20260910_BUF@KC"
        self.rows = [
            {"team_id": "12", "metric": "total_yards"},
            {"team_id": "2", "metric": "total_yards"},
        ]
        self.patches = [
            patch.object(stats.bigquery, "Client", return_value=self.client),
            patch.object(
                stats,
                "fetch_games_to_process",
                return_value=[{"gameID": self.game_id}],
            ),
            patch.object(stats, "fetch_and_validate_api_data", return_value={"body": {}}),
            patch.object(stats, "save_raw_response"),
            patch.object(stats, "parse_game_stats", return_value=self.rows),
            patch.object(stats, "log_event"),
            patch.object(stats.time, "sleep"),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_invalid_response_inserts_nothing_and_writes_no_marker(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                stats,
                "validate_nfl_boxscore",
                return_value=Validation(
                    accepted=False,
                    code="game_not_final",
                    reason="not final",
                    team_ids=(),
                ),
            ))
            reconcile = stack.enter_context(
                patch.object(stats, "reconcile_game_rows")
            )
            mark = stack.enter_context(
                patch.object(stats, "mark_game_as_loaded")
            )
            result = stats.fetch_nfl_stats()

        self.assertEqual(result, 0)
        reconcile.assert_not_called()
        mark.assert_not_called()

    def test_valid_response_confirms_rows_before_marker(self):
        events = []
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                stats,
                "validate_nfl_boxscore",
                return_value=Validation(accepted=True),
            ))
            stack.enter_context(patch.object(
                stats,
                "reconcile_game_rows",
                side_effect=lambda *args: events.append("rows") or True,
            ))
            stack.enter_context(patch.object(
                stats,
                "mark_game_as_loaded",
                side_effect=lambda *args: events.append("marker"),
            ))
            result = stats.fetch_nfl_stats(load_date="2026-09-10")

        self.assertEqual(result, 1)
        self.assertEqual(events, ["rows", "marker"])

    def test_insert_failure_writes_no_marker_and_counts_no_game(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                stats,
                "validate_nfl_boxscore",
                return_value=Validation(accepted=True),
            ))
            stack.enter_context(patch.object(
                stats,
                "reconcile_game_rows",
                side_effect=RuntimeError("insert failed"),
            ))
            mark = stack.enter_context(
                patch.object(stats, "mark_game_as_loaded")
            )
            result = stats.fetch_nfl_stats()

        self.assertEqual(result, 0)
        mark.assert_not_called()

    def test_marker_failure_counts_no_game(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                stats,
                "validate_nfl_boxscore",
                return_value=Validation(accepted=True),
            ))
            stack.enter_context(
                patch.object(stats, "reconcile_game_rows", return_value=True)
            )
            stack.enter_context(patch.object(
                stats,
                "mark_game_as_loaded",
                side_effect=RuntimeError("marker failed"),
            ))
            result = stats.fetch_nfl_stats()

        self.assertEqual(result, 0)

    def test_retry_with_complete_rows_does_not_insert_duplicates(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                stats,
                "fetch_stored_team_ids",
                return_value={"12", "2"},
            ))
            insert = stack.enter_context(
                patch.object(stats, "insert_rows_bq")
            )
            delete = stack.enter_context(
                patch.object(stats, "delete_game_rows")
            )
            inserted = stats.reconcile_game_rows(
                self.client,
                self.game_id,
                self.rows,
                {"12", "2"},
            )

        self.assertFalse(inserted)
        insert.assert_not_called()
        delete.assert_not_called()

    def test_rejected_game_can_succeed_on_later_retry(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(
                stats,
                "fetch_games_to_process",
                side_effect=[
                    [{"gameID": self.game_id}],
                    [{"gameID": self.game_id}],
                ],
            ))
            stack.enter_context(patch.object(
                stats,
                "validate_nfl_boxscore",
                side_effect=[
                    Validation(
                        accepted=False,
                        code="game_not_final",
                        reason="not final",
                        team_ids=(),
                    ),
                    Validation(accepted=True),
                ],
            ))
            stack.enter_context(
                patch.object(stats, "reconcile_game_rows", return_value=True)
            )
            mark = stack.enter_context(
                patch.object(stats, "mark_game_as_loaded")
            )
            first_result = stats.fetch_nfl_stats()
            second_result = stats.fetch_nfl_stats()

        self.assertEqual((first_result, second_result), (0, 1))
        mark.assert_called_once_with(self.client, self.game_id)


class StatsHelperTests(unittest.TestCase):
    def test_partial_existing_rows_are_replaced_and_confirmed(self):
        client = MagicMock()
        game_id = "20260910_BUF@KC"
        rows = [
            {"team_id": "12", "metric": "total_yards"},
            {"team_id": "2", "metric": "total_yards"},
        ]

        with ExitStack() as stack:
            stack.enter_context(patch.object(
                stats,
                "fetch_stored_team_ids",
                side_effect=[{"12"}, {"12", "2"}],
            ))
            delete = stack.enter_context(
                patch.object(stats, "delete_game_rows")
            )
            insert = stack.enter_context(
                patch.object(stats, "insert_rows_bq")
            )
            inserted = stats.reconcile_game_rows(
                client,
                game_id,
                rows,
                {"12", "2"},
            )

        self.assertTrue(inserted)
        delete.assert_called_once_with(client, game_id)
        insert.assert_called_once_with(client, rows)

    def test_marker_insert_errors_raise(self):
        client = MagicMock()
        client.insert_rows_json.return_value = [{"reason": "test"}]

        with patch.object(stats, "log_event"):
            with self.assertRaises(RuntimeError):
                stats.mark_game_as_loaded(client, "20260910_BUF@KC")


if __name__ == "__main__":
    unittest.main()
