import unittest
from dataclasses import replace
from datetime import datetime
from unittest.mock import MagicMock, patch

import app as app_module
from api_calls import api_call_nfl_games as games


class TestApp(unittest.TestCase):
    @staticmethod
    def _record_call(call_order, name, result=None, error=None):
        def api_call(load_date=None):
            call_order.append((name, load_date))
            if error is not None:
                raise error
            return result

        return api_call

    def _api_calls(
        self,
        call_order,
        stats_result=0,
        scores_result=None,
        schedule_result=None,
        schedule_error=None,
        stats_error=None,
        scores_error=None,
    ):
        return [
            {
                "name": "NFL Game Schedule API Call",
                "function": self._record_call(
                    call_order,
                    "schedule",
                    result=schedule_result,
                    error=schedule_error,
                ),
                "max_cycles": 1,
            },
            {
                "name": "NFL Stats API Call",
                "function": self._record_call(
                    call_order,
                    "stats",
                    result=stats_result,
                    error=stats_error,
                ),
                "max_cycles": 1,
            },
            {
                "name": "NFL Scores API Call",
                "function": self._record_call(
                    call_order,
                    "scores",
                    result=scores_result,
                    error=scores_error,
                ),
                "max_cycles": 1,
            },
        ]

    @staticmethod
    def _successful_pipeline_summary():
        return {
            "season": "2026",
            "status": "success",
            "failed_stage": None,
            "stages": {
                "facts": {"status": "completed", "row_count": 2},
                "windowed_metrics": {
                    "status": "completed",
                    "row_count": 3,
                },
                "rankings": {"status": "completed", "row_count": 4},
            },
        }

    @staticmethod
    def _ingestion_result(
        *,
        status="success",
        game_ids=("20260806_CLE@CHI",),
        successful_games=1,
        failures=(),
        no_op_reason=None,
    ):
        result = {
            "status": status,
            "selected_game_count": len(game_ids),
            "selected_game_ids": list(game_ids),
            "successful_game_count": successful_games,
            "failed_game_count": len(failures),
            "failures": list(failures),
        }
        if no_op_reason is not None:
            result["no_op_reason"] = no_op_reason
        return result

    def test_zero_accepted_stats_is_successful_no_op(self):
        call_order = []
        api_calls = self._api_calls(
            call_order,
            stats_result=self._ingestion_result(
                status="no_op",
                game_ids=(),
                successful_games=0,
                no_op_reason="no_eligible_games",
            ),
            scores_result=self._ingestion_result(
                status="no_op",
                game_ids=(),
                successful_games=0,
                no_op_reason="no_eligible_games",
            ),
        )

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
                create=True,
            ) as conductor,
        ):
            summary = app_module.run_api_calls(load_date="2026-08-06")

        self.assertEqual(
            call_order,
            [
                ("schedule", "2026-08-06"),
                ("stats", "2026-08-06"),
                ("scores", "2026-08-06"),
            ],
        )
        conductor.assert_not_called()
        self.assertEqual(summary["status"], "no_op")
        self.assertEqual(summary["execution_mode"], "daily")
        self.assertEqual(summary["active_season"], "2026")
        self.assertEqual(summary["selected_game_count"], 0)
        self.assertEqual(summary["selected_game_ids"], [])
        self.assertEqual(summary["no_op_reason"], "no_accepted_stats_games")
        self.assertEqual(summary["accepted_stats_games"], 0)
        self.assertEqual(summary["metric_pipeline"]["status"], "skipped")
        self.assertEqual(
            summary["metric_pipeline"]["reason"],
            "no_accepted_stats_games",
        )

    def test_positive_accepted_stats_runs_2026_conductor_with_writes(self):
        call_order = []
        game_ids = ("20260806_CLE@CHI", "20260806_LV@SEA")
        api_calls = self._api_calls(
            call_order,
            stats_result=self._ingestion_result(
                game_ids=game_ids,
                successful_games=2,
            ),
            scores_result=self._ingestion_result(
                game_ids=game_ids,
                successful_games=2,
            ),
        )

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
                create=True,
                return_value=self._successful_pipeline_summary(),
            ) as conductor,
            patch(
                "agg.aggregate_nfl_metrics_2025.run_aggregate_for_season"
            ) as legacy_aggregate,
        ):
            summary = app_module.run_api_calls(load_date="2026-08-06")

        self.assertEqual(
            call_order,
            [
                ("schedule", "2026-08-06"),
                ("stats", "2026-08-06"),
                ("scores", "2026-08-06"),
            ],
        )
        conductor.assert_called_once_with(season="2026", write=True)
        legacy_aggregate.assert_not_called()
        self.assertEqual(summary["status"], "success")
        self.assertEqual(summary["execution_mode"], "daily")
        self.assertEqual(summary["active_season"], "2026")
        self.assertEqual(summary["selected_game_count"], 2)
        self.assertEqual(summary["selected_game_ids"], list(game_ids))
        self.assertEqual(summary["accepted_stats_games"], 2)
        self.assertEqual(
            summary["ingestion"]["NFL Scores API Call"][
                "successful_game_count"
            ],
            2,
        )
        self.assertEqual(summary["metric_pipeline"]["status"], "success")
        self.assertEqual(
            summary["metric_pipeline"]["stages"]["rankings"]["row_count"],
            4,
        )

    def test_configured_2025_season_reaches_conductor(self):
        call_order = []
        api_calls = self._api_calls(
            call_order,
            stats_result=self._ingestion_result(
                game_ids=("20250914_ATL@MIN",),
            ),
            scores_result=self._ingestion_result(
                game_ids=("20250914_ATL@MIN",),
            ),
        )
        replay_config = replace(
            app_module.RUNTIME_CONFIG,
            environment="dev",
            run_mode="controlled_replay",
            active_season="2025",
            replay_date="2025-09-14",
        )
        pipeline_summary = {
            "season": "2025",
            "status": "success",
            "failed_stage": None,
            "stages": {},
        }

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(app_module, "RUNTIME_CONFIG", replay_config),
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
                create=True,
                return_value=pipeline_summary,
            ) as conductor,
        ):
            summary = app_module.run_api_calls(load_date="2025-09-14")

        conductor.assert_called_once_with(season="2025", write=True)
        self.assertEqual(summary["status"], "success")
        self.assertEqual(summary["execution_mode"], "controlled_replay")
        self.assertEqual(summary["active_season"], "2025")
        self.assertEqual(summary["selected_game_ids"], ["20250914_ATL@MIN"])
        self.assertEqual(summary["metric_pipeline"]["season"], "2025")

    def test_stats_internal_failure_is_visible_and_skips_conductor(self):
        call_order = []
        failure = {
            "game_id": "20260806_CLE@CHI",
            "error": "game_not_final - not final",
        }
        api_calls = self._api_calls(
            call_order,
            stats_result=self._ingestion_result(
                status="failed",
                successful_games=0,
                failures=(failure,),
            ),
            scores_result=self._ingestion_result(),
        )

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(app_module, "run_gamelens_metric_pipeline") as conductor,
        ):
            summary = app_module.run_api_calls(load_date="2026-08-06")

        conductor.assert_not_called()
        self.assertEqual(summary["status"], "partial_failure")
        self.assertEqual(summary["accepted_stats_games"], 0)
        stats_summary = summary["ingestion"]["NFL Stats API Call"]
        self.assertEqual(stats_summary["status"], "failed")
        self.assertEqual(stats_summary["failed_game_count"], 1)
        self.assertEqual(stats_summary["failures"], [failure])

    def test_schedule_internal_failure_is_visible(self):
        call_order = []
        schedule_result = {
            "status": "partial_failure",
            "requested_dates": ["20260805", "20260806"],
            "successful_dates": ["20260806"],
            "no_op_dates": [],
            "selected_game_count": 1,
            "selected_game_ids": ["20260806_CLE@CHI"],
            "successful_game_count": 1,
            "inserted_row_count": 1,
            "failed_date_count": 1,
            "failures": [{
                "game_date": "20260805",
                "error": "schedule unavailable",
            }],
        }
        api_calls = self._api_calls(
            call_order,
            schedule_result=schedule_result,
            stats_result=self._ingestion_result(
                status="no_op",
                game_ids=(),
                successful_games=0,
                no_op_reason="no_eligible_games",
            ),
            scores_result=self._ingestion_result(
                status="no_op",
                game_ids=(),
                successful_games=0,
                no_op_reason="no_eligible_games",
            ),
        )

        with patch.object(
            app_module,
            "API_CALLS",
            api_calls,
        ), patch.object(
            app_module,
            "run_gamelens_metric_pipeline",
        ) as conductor:
            summary = app_module.run_api_calls(load_date="2026-08-06")

        conductor.assert_not_called()
        self.assertEqual(summary["status"], "partial_failure")
        self.assertEqual(
            summary["ingestion"]["NFL Game Schedule API Call"],
            schedule_result,
        )

    def test_stats_exception_is_visible_and_skips_conductor(self):
        call_order = []
        api_calls = self._api_calls(
            call_order,
            stats_error=RuntimeError("stats unavailable"),
        )

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
                create=True,
            ) as conductor,
        ):
            summary = app_module.run_api_calls(load_date="2026-08-06")

        self.assertEqual(
            call_order,
            [
                ("schedule", "2026-08-06"),
                ("stats", "2026-08-06"),
                ("scores", "2026-08-06"),
            ],
        )
        conductor.assert_not_called()
        self.assertEqual(summary["status"], "partial_failure")
        self.assertEqual(summary["accepted_stats_games"], 0)
        self.assertEqual(
            summary["ingestion"]["NFL Stats API Call"]["status"],
            "failed",
        )
        self.assertEqual(
            summary["ingestion"]["NFL Stats API Call"]["error"],
            "stats unavailable",
        )

    def test_scores_exception_remains_visible_when_pipeline_succeeds(self):
        call_order = []
        api_calls = self._api_calls(
            call_order,
            stats_result=1,
            scores_error=RuntimeError("scores unavailable"),
        )

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
                create=True,
                return_value=self._successful_pipeline_summary(),
            ) as conductor,
        ):
            summary = app_module.run_api_calls(load_date="2026-08-06")

        conductor.assert_called_once_with(season="2026", write=True)
        self.assertEqual(summary["status"], "partial_failure")
        self.assertEqual(summary["accepted_stats_games"], 1)
        self.assertEqual(summary["metric_pipeline"]["status"], "success")
        self.assertEqual(
            summary["ingestion"]["NFL Scores API Call"]["status"],
            "failed",
        )

    def test_scores_internal_failure_remains_visible(self):
        call_order = []
        failure = {
            "game_id": "20260806_CLE@CHI",
            "error": "score payload missing",
        }
        api_calls = self._api_calls(
            call_order,
            stats_result=self._ingestion_result(),
            scores_result=self._ingestion_result(
                status="failed",
                successful_games=0,
                failures=(failure,),
            ),
        )

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
                return_value=self._successful_pipeline_summary(),
            ),
        ):
            summary = app_module.run_api_calls(load_date="2026-08-06")

        self.assertEqual(summary["status"], "partial_failure")
        scores_summary = summary["ingestion"]["NFL Scores API Call"]
        self.assertEqual(scores_summary["status"], "failed")
        self.assertEqual(scores_summary["failed_game_count"], 1)
        self.assertEqual(scores_summary["failures"], [failure])

    def test_conductor_reported_failure_is_not_success(self):
        call_order = []
        api_calls = self._api_calls(call_order, stats_result=1)
        failed_pipeline = {
            "season": "2026",
            "status": "failed",
            "failed_stage": "facts",
            "stages": {
                "facts": {"status": "failed", "row_count": None},
                "windowed_metrics": {"status": "skipped", "row_count": None},
                "rankings": {"status": "skipped", "row_count": None},
            },
        }

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
                create=True,
                return_value=failed_pipeline,
            ),
        ):
            summary = app_module.run_api_calls(load_date="2026-08-06")

        self.assertEqual(summary["status"], "failure")
        self.assertEqual(summary["metric_pipeline"]["status"], "failed")
        self.assertEqual(summary["metric_pipeline"]["failed_stage"], "facts")

    def test_conductor_exception_is_not_success(self):
        call_order = []
        api_calls = self._api_calls(call_order, stats_result=1)

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
                create=True,
                side_effect=RuntimeError("pipeline crashed"),
            ),
        ):
            summary = app_module.run_api_calls(load_date="2026-08-06")

        self.assertEqual(summary["status"], "failure")
        self.assertEqual(summary["metric_pipeline"]["status"], "failed")
        self.assertEqual(summary["metric_pipeline"]["error"], "pipeline crashed")

    def test_empty_schedule_cycle_returns_http_200_and_skips_metrics(self):
        daily_config = replace(
            app_module.RUNTIME_CONFIG,
            environment="dev",
            run_mode="daily",
            active_season="2026",
            replay_date=None,
        )
        schedule_clock = MagicMock()
        schedule_clock.now.return_value = datetime(2026, 8, 1, 8, 0, 0)
        call_order = []

        def schedule_call(load_date=None):
            call_order.append(("schedule", load_date))
            return games.fetch_nfl_games(load_date=load_date)

        no_eligible_games = self._ingestion_result(
            status="no_op",
            game_ids=(),
            successful_games=0,
            no_op_reason="no_eligible_games",
        )
        api_calls = [
            {
                "name": "NFL Game Schedule API Call",
                "function": schedule_call,
                "max_cycles": 1,
            },
            {
                "name": "NFL Stats API Call",
                "function": self._record_call(
                    call_order,
                    "stats",
                    result=no_eligible_games,
                ),
                "max_cycles": 1,
            },
            {
                "name": "NFL Scores API Call",
                "function": self._record_call(
                    call_order,
                    "scores",
                    result=no_eligible_games,
                ),
                "max_cycles": 1,
            },
        ]

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(app_module, "RUNTIME_CONFIG", daily_config),
            patch.object(games, "RUNTIME_CONFIG", daily_config),
            patch.object(games, "datetime", schedule_clock),
            patch.object(games, "get_secret", return_value="test-key"),
            patch.object(
                games,
                "fetch_games_for_date",
                return_value=[],
            ) as fetch,
            patch.object(games, "save_raw_response"),
            patch.object(games, "bq") as bq,
            patch.object(games, "insert_into_bigquery") as insert,
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
            ) as conductor,
            app_module.app.test_client() as client,
        ):
            response = client.post("/", json={})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "no_op")
        self.assertEqual(
            payload["no_op_reason"],
            "no_accepted_stats_games",
        )
        schedule_summary = payload["ingestion"][
            "NFL Game Schedule API Call"
        ]
        self.assertEqual(schedule_summary["status"], "no_op")
        self.assertEqual(schedule_summary["dates_checked"], 4)
        self.assertEqual(schedule_summary["dates_with_games"], 0)
        self.assertEqual(schedule_summary["dates_with_no_games"], 4)
        self.assertEqual(schedule_summary["failed_date_count"], 0)
        self.assertEqual(
            payload["ingestion"]["NFL Stats API Call"]["status"],
            "no_op",
        )
        self.assertEqual(
            payload["ingestion"]["NFL Scores API Call"]["status"],
            "no_op",
        )
        self.assertEqual(
            payload["metric_pipeline"]["status"],
            "skipped",
        )
        self.assertEqual(
            call_order,
            [
                ("schedule", None),
                ("stats", None),
                ("scores", None),
            ],
        )
        self.assertEqual(fetch.call_count, 4)
        bq.query.assert_not_called()
        insert.assert_not_called()
        conductor.assert_not_called()


    def test_scheduler_route_returns_summary_and_truthful_http_status(self):
        cases = (
            ("success", 200),
            ("no_op", 200),
            ("partial_failure", 500),
            ("failure", 500),
        )

        with app_module.app.test_client() as client:
            for status, expected_http_status in cases:
                with self.subTest(status=status):
                    summary = {
                        "status": status,
                        "accepted_stats_games": 0,
                        "metric_pipeline": {"status": "skipped"},
                    }
                    with patch.object(
                        app_module,
                        "setup_schedules",
                        return_value=summary,
                    ) as setup:
                        response = client.post(
                            "/",
                            json={"load_date": "2026-08-06"},
                        )

                    self.assertEqual(
                        response.status_code,
                        expected_http_status,
                    )
                    self.assertEqual(response.get_json(), summary)
                    setup.assert_called_once_with(load_date="2026-08-06")


if __name__ == "__main__":
    unittest.main()
