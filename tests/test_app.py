import unittest
from unittest.mock import patch

import app as app_module


class TestApp(unittest.TestCase):
    def test_zero_accepted_stats_is_successful_no_op(self):
        call_order = []

        def record_call(name, result=None):
            def api_call(load_date=None):
                call_order.append((name, load_date))
                return result

            return api_call

        api_calls = [
            {
                "name": "NFL Game Schedule API Call",
                "function": record_call("schedule"),
                "max_cycles": 1,
            },
            {
                "name": "NFL Stats API Call",
                "function": record_call("stats", 0),
                "max_cycles": 1,
            },
            {
                "name": "NFL Scores API Call",
                "function": record_call("scores"),
                "max_cycles": 1,
            },
        ]

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

        def record_call(name, result=None):
            def api_call(load_date=None):
                call_order.append((name, load_date))
                return result

            return api_call

        api_calls = [
            {
                "name": "NFL Game Schedule API Call",
                "function": record_call("schedule"),
                "max_cycles": 1,
            },
            {
                "name": "NFL Stats API Call",
                "function": record_call("stats", 2),
                "max_cycles": 1,
            },
            {
                "name": "NFL Scores API Call",
                "function": record_call("scores"),
                "max_cycles": 1,
            },
        ]

        with (
            patch.object(app_module, "API_CALLS", api_calls),
            patch.object(
                app_module,
                "run_gamelens_metric_pipeline",
                create=True,
            ) as conductor,
            patch(
                "agg.aggregate_nfl_metrics_2025.run_aggregate_for_season"
            ) as legacy_aggregate,
        ):
            app_module.run_api_calls(load_date="2026-08-06")

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


if __name__ == "__main__":
    unittest.main()
