import unittest
from unittest.mock import patch

import app as app_module


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
            "stages": {},
        }

    def test_zero_accepted_stats_is_successful_no_op(self):
        call_order = []
        api_calls = self._api_calls(call_order, stats_result=0)

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
        self.assertEqual(summary["accepted_stats_games"], 0)
        self.assertEqual(summary["metric_pipeline"]["status"], "skipped")
        self.assertEqual(
            summary["metric_pipeline"]["reason"],
            "no_accepted_stats_games",
        )

    def test_positive_accepted_stats_runs_2026_conductor_with_writes(self):
        call_order = []
        api_calls = self._api_calls(call_order, stats_result=2)

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
        self.assertEqual(summary["accepted_stats_games"], 2)
        self.assertEqual(summary["metric_pipeline"]["status"], "success")

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
