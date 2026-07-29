"""Conduct the GameLens metric builders in their required order."""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from agg.build_metric_facts import run_build_game_team_metric_facts
from agg.build_metric_rankings import run_build_team_metric_rankings
from agg.build_windowed_metrics import run_build_windowed_metrics
from utils.logging_setup import log_event, setup_logging


STAGE_ORDER = ("facts", "windowed_metrics", "rankings")


def _new_summary(season: str) -> Dict[str, Any]:
    return {
        "season": season,
        "status": "pending",
        "failed_stage": None,
        "stages": {
            stage: {"status": "pending", "row_count": None}
            for stage in STAGE_ORDER
        },
    }


def _validate_stage_result(stage: str, result: Any) -> pd.DataFrame:
    if not isinstance(result, pd.DataFrame):
        raise TypeError(
            f"{stage} returned {type(result).__name__}; expected pandas.DataFrame"
        )
    if result.empty:
        raise ValueError(f"{stage} returned an empty DataFrame")
    return result


def run_gamelens_metric_pipeline(
    season: str,
    if_exists: str = "replace",
    write: bool = True,
) -> Dict[str, Any]:
    """Run Facts -> Windowed Metrics -> Rankings for one explicit season."""
    setup_logging()
    season = str(season).strip()
    if not season:
        raise ValueError("season is required")

    summary = _new_summary(season)
    builders = (
        (
            "facts",
            run_build_game_team_metric_facts,
            {"season": season, "if_exists": if_exists, "write": write},
        ),
        (
            "windowed_metrics",
            run_build_windowed_metrics,
            {"season": season, "if_exists": if_exists, "write": write},
        ),
        (
            "rankings",
            run_build_team_metric_rankings,
            {
                "season": season,
                "if_exists": if_exists,
                "write": write,
                "recreate_table": False,
            },
        ),
    )

    log_event(
        "info",
        "gamelens_metric_pipeline_started",
        season=season,
        if_exists=if_exists,
        write=write,
    )

    for stage_index, (stage, builder, kwargs) in enumerate(builders):
        try:
            result = _validate_stage_result(stage, builder(**kwargs))
        except Exception as exc:
            summary["status"] = "failed"
            summary["failed_stage"] = stage
            summary["stages"][stage]["status"] = "failed"
            for skipped_stage, _, _ in builders[stage_index + 1 :]:
                summary["stages"][skipped_stage]["status"] = "skipped"
            log_event(
                "error",
                "gamelens_metric_pipeline_stage_failed",
                season=season,
                stage=stage,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return summary

        summary["stages"][stage] = {
            "status": "completed",
            "row_count": len(result),
        }

    summary["status"] = "success"
    log_event(
        "info",
        "gamelens_metric_pipeline_complete",
        season=season,
        write=write,
        stages=summary["stages"],
    )
    return summary
