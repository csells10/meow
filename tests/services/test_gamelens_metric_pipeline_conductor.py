import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd


def _install_import_stubs() -> None:
    repo_root = Path(__file__).resolve().parents[2]

    if (
        (repo_root / "agg" / "build_metric_facts.py").exists()
        and (repo_root / "agg" / "build_metric_rankings.py").exists()
        and (repo_root / "agg" / "build_windowed_metrics.py").exists()
        and (repo_root / "utils" / "logging_setup.py").exists()
    ):
        return

    agg_module = types.ModuleType("agg")
    agg_module.__path__ = []

    facts_module = types.ModuleType("agg.build_metric_facts")
    facts_module.run_build_game_team_metric_facts = MagicMock()
    rankings_module = types.ModuleType("agg.build_metric_rankings")
    rankings_module.run_build_team_metric_rankings = MagicMock()
    windowed_module = types.ModuleType("agg.build_windowed_metrics")
    windowed_module.run_build_windowed_metrics = MagicMock()

    logging_module = types.ModuleType("utils.logging_setup")
    logging_module.log_event = MagicMock()
    logging_module.setup_logging = MagicMock()
    utils_module = types.ModuleType("utils")
    utils_module.__path__ = []

    sys.modules.setdefault("agg", agg_module)
    sys.modules.setdefault("agg.build_metric_facts", facts_module)
    sys.modules.setdefault("agg.build_metric_rankings", rankings_module)
    sys.modules.setdefault("agg.build_windowed_metrics", windowed_module)
    sys.modules.setdefault("utils", utils_module)
    sys.modules.setdefault("utils.logging_setup", logging_module)


_install_import_stubs()

from services import gamelens_metric_pipeline_conductor as conductor


def _df(rows: int) -> pd.DataFrame:
    return pd.DataFrame({"value": range(rows)})


class GameLensMetricPipelineConductorTests(unittest.TestCase):
    def setUp(self):
        self.facts = patch.object(
            conductor,
            "run_build_game_team_metric_facts",
            return_value=_df(2),
        ).start()
        self.windowed = patch.object(
            conductor,
            "run_build_windowed_metrics",
            return_value=_df(3),
        ).start()
        self.rankings = patch.object(
            conductor,
            "run_build_team_metric_rankings",
            return_value=_df(4),
        ).start()
        self.addCleanup(patch.stopall)

    def test_success_calls_builders_in_order_and_returns_counts(self):
        call_order = []
        self.facts.side_effect = lambda **kwargs: call_order.append("facts") or _df(2)
        self.windowed.side_effect = (
            lambda **kwargs: call_order.append("windowed_metrics") or _df(3)
        )
        self.rankings.side_effect = (
            lambda **kwargs: call_order.append("rankings") or _df(4)
        )

        summary = conductor.run_gamelens_metric_pipeline(
            season="2025",
            if_exists="replace",
            write=False,
        )

        self.assertEqual(call_order, ["facts", "windowed_metrics", "rankings"])
        self.facts.assert_called_once_with(
            season="2025", if_exists="replace", write=False
        )
        self.windowed.assert_called_once_with(
            season="2025", if_exists="replace", write=False
        )
        self.rankings.assert_called_once_with(
            season="2025",
            if_exists="replace",
            write=False,
            recreate_table=False,
        )
        self.assertEqual(summary["status"], "success")
        self.assertIsNone(summary["failed_stage"])
        self.assertEqual(
            summary["stages"],
            {
                "facts": {"status": "completed", "row_count": 2},
                "windowed_metrics": {"status": "completed", "row_count": 3},
                "rankings": {"status": "completed", "row_count": 4},
            },
        )

    def test_facts_exception_skips_later_stages(self):
        self.facts.side_effect = RuntimeError("facts failed")

        summary = conductor.run_gamelens_metric_pipeline("2025", write=False)

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["failed_stage"], "facts")
        self.assertEqual(summary["stages"]["facts"]["status"], "failed")
        self.assertEqual(
            summary["stages"]["windowed_metrics"]["status"], "skipped"
        )
        self.assertEqual(summary["stages"]["rankings"]["status"], "skipped")
        self.windowed.assert_not_called()
        self.rankings.assert_not_called()

    def test_empty_facts_result_stops_pipeline(self):
        self.facts.return_value = pd.DataFrame()

        summary = conductor.run_gamelens_metric_pipeline("2025", write=False)

        self.assertEqual(summary["failed_stage"], "facts")
        self.assertEqual(summary["stages"]["facts"]["status"], "failed")
        self.windowed.assert_not_called()
        self.rankings.assert_not_called()

    def test_non_dataframe_result_stops_pipeline(self):
        self.facts.return_value = []

        summary = conductor.run_gamelens_metric_pipeline("2025", write=False)

        self.assertEqual(summary["failed_stage"], "facts")
        self.assertEqual(summary["stages"]["facts"]["status"], "failed")
        self.windowed.assert_not_called()
        self.rankings.assert_not_called()

    def test_windowed_failure_preserves_facts_and_skips_rankings(self):
        self.windowed.side_effect = RuntimeError("windowed failed")

        summary = conductor.run_gamelens_metric_pipeline("2025", write=False)

        self.assertEqual(summary["failed_stage"], "windowed_metrics")
        self.assertEqual(
            summary["stages"]["facts"],
            {"status": "completed", "row_count": 2},
        )
        self.assertEqual(
            summary["stages"]["windowed_metrics"]["status"], "failed"
        )
        self.assertEqual(summary["stages"]["rankings"]["status"], "skipped")
        self.rankings.assert_not_called()

    def test_rankings_failure_preserves_completed_stages(self):
        self.rankings.side_effect = RuntimeError("rankings failed")

        summary = conductor.run_gamelens_metric_pipeline("2025", write=False)

        self.assertEqual(summary["failed_stage"], "rankings")
        self.assertEqual(
            summary["stages"]["facts"],
            {"status": "completed", "row_count": 2},
        )
        self.assertEqual(
            summary["stages"]["windowed_metrics"],
            {"status": "completed", "row_count": 3},
        )
        self.assertEqual(summary["stages"]["rankings"]["status"], "failed")

    def test_requires_nonblank_explicit_season(self):
        with self.assertRaisesRegex(ValueError, "season is required"):
            conductor.run_gamelens_metric_pipeline(" ", write=False)

        self.facts.assert_not_called()
        self.windowed.assert_not_called()
        self.rankings.assert_not_called()


if __name__ == "__main__":
    unittest.main()
