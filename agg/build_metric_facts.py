"""
Build cleaned GameLens game/team/metric fact table.

Purpose
-------
Read Analytics.game_metrics_flat, ignore legacy parser category/core_area,
attach authoritative metadata from analytics.metric_registry.py, join League.schedule,
and write Analytics.game_team_metric_facts_{season}.

This script does not change the live /game API path and does not re-call the
external NFL API.

Important schema note
---------------------
The destination table should already exist with the explicit schema created by:

    recreate_gamelens_metric_tables.py

That schema includes lens_tags as REPEATED STRING. This builder uses the
existing BigQuery table schema during load so repeated fields are preserved.
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from utils.logging_setup import log_event, setup_logging

# Recommended location for the registry:
#   analytics/metric_registry.py
# If you store it somewhere else, update this import only.
from analytics.metric_registry import (  # type: ignore
    METRIC_REGISTRY,
    get_metric_meta,
    validate_metric_registry,
)
from runtime_config import load_runtime_config


RUNTIME_CONFIG = load_runtime_config()
PROJECT = RUNTIME_CONFIG.project_id
SOURCE_METRICS_TABLE = RUNTIME_CONFIG.analytics_object("game_metrics_flat")
SCHEDULE_TABLE = RUNTIME_CONFIG.league_object("schedule")
OUTPUT_TABLE_TEMPLATE = RUNTIME_CONFIG.analytics_object(
    "game_team_metric_facts_{season}"
)

FINAL_STATUSES = ["Final", "Final/OT"]
DEDUP_KEEP = "last"

OUTPUT_COLUMNS = [
    "season",
    "game_id",
    "game_date",
    "game_datetime_raw",
    "game_week",
    "season_type",
    "season_phase",
    "phase_week",
    "global_week_order",
    "is_preseason",
    "is_regular_season",
    "is_postseason",
    "is_playoff_game",
    "team_id",
    "team_abv",
    "team_type",
    "metric",
    "value",

    # Registry metadata
    "label",
    "definition",
    "category",
    "core_area",
    "comparison_direction",
    "higher_is_better",
    "raw_or_derived",
    "aggregation_method",
    "numerator",
    "denominator",
    "format",
    "decimals",
    "notes",
    "ranking_usage",
    "signal_strength",
    "edge_language_allowed",
    "include_in_core_area_advantage",
    "confidence_eligible",
    "data_quality_status",
    "lens_tags",

    "created_at",
]


# -----------------------------------------------------------------------------
# Schedule phase normalization
# -----------------------------------------------------------------------------

def normalize_game_week(game_week: Optional[str]) -> Dict[str, Any]:
    """Normalize League.schedule.gameWeek into sortable phase fields.

    Expected examples:
      - Preseason Week 1
      - Week 1
      - Week 18
      - Wild Card
      - Divisional Round
      - Conference Championship
      - Super Bowl
    """
    raw = (game_week or "").strip()
    lowered = raw.lower()

    preseason_match = re.search(r"preseason\s+week\s+(\d+)", lowered)
    if preseason_match:
        week = int(preseason_match.group(1))
        return {
            "season_phase": "preseason",
            "phase_week": week,
            "global_week_order": week,
            "is_preseason": True,
            "is_regular_season": False,
            "is_postseason": False,
            "is_playoff_game": False,
        }

    regular_match = re.fullmatch(r"week\s+(\d+)", lowered)
    if regular_match:
        week = int(regular_match.group(1))
        return {
            "season_phase": "regular_season",
            "phase_week": week,
            "global_week_order": 100 + week,
            "is_preseason": False,
            "is_regular_season": True,
            "is_postseason": False,
            "is_playoff_game": False,
        }

    postseason_map = {
        "wild card": (1, 201),
        "wildcard": (1, 201),
        "divisional round": (2, 202),
        "conference championship": (3, 203),
        "super bowl": (4, 204),
    }

    if lowered in postseason_map:
        phase_week, global_week_order = postseason_map[lowered]
        return {
            "season_phase": "postseason",
            "phase_week": phase_week,
            "global_week_order": global_week_order,
            "is_preseason": False,
            "is_regular_season": False,
            "is_postseason": True,
            "is_playoff_game": True,
        }

    return {
        "season_phase": "unknown",
        "phase_week": None,
        "global_week_order": None,
        "is_preseason": False,
        "is_regular_season": False,
        "is_postseason": False,
        "is_playoff_game": False,
    }


# -----------------------------------------------------------------------------
# BigQuery load
# -----------------------------------------------------------------------------

def load_game_metric_rows(client: bigquery.Client, season: str) -> pd.DataFrame:
    """Load source rows and schedule context.

    Important:
    This query intentionally does NOT select m.category or m.core_area.
    Those are legacy parser fields and are not authoritative.
    """
    query = f"""
        SELECT
            CAST(s.season AS STRING) AS season,
            m.gameID AS game_id,
            DATE(s.gameDate) AS game_date,
            CAST(s.gameTime_epoch AS STRING) AS game_datetime_raw,
            s.gameWeek AS game_week,
            s.seasonType AS season_type,
            s.gameStatus AS game_status,
            CAST(s.teamIDHome AS STRING) AS home_team_id,
            CAST(s.teamIDAway AS STRING) AS away_team_id,
            s.home AS home_team_abv,
            s.away AS away_team_abv,
            CAST(m.team_id AS STRING) AS team_id,
            m.team_abv AS team_abv,
            m.metric AS metric,
            SAFE_CAST(m.value AS FLOAT64) AS value
        FROM `{PROJECT}.{SOURCE_METRICS_TABLE}` AS m
        INNER JOIN `{PROJECT}.{SCHEDULE_TABLE}` AS s
            ON m.gameID = s.gameID
        WHERE CAST(s.season AS STRING) = @season
          AND s.gameStatus IN UNNEST(@final_statuses)
          AND m.metric IS NOT NULL
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("season", "STRING", season),
            bigquery.ArrayQueryParameter("final_statuses", "STRING", FINAL_STATUSES),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()
    log_event("info", "fact_source_rows_loaded", season=season, rows=len(df))
    return df


# -----------------------------------------------------------------------------
# Transform
# -----------------------------------------------------------------------------

def attach_team_type(df: pd.DataFrame) -> pd.DataFrame:
    """Add home/away team_type using schedule team IDs."""
    df = df.copy()

    team_id = df["team_id"].astype(str)
    home_team_id = df["home_team_id"].astype(str)
    away_team_id = df["away_team_id"].astype(str)

    df["team_type"] = None
    df.loc[team_id == home_team_id, "team_type"] = "home"
    df.loc[team_id == away_team_id, "team_type"] = "away"

    unknown_count = int(df["team_type"].isna().sum())
    if unknown_count:
        log_event(
            "warning",
            "team_type_unresolved",
            rows=unknown_count,
            sample=df.loc[df["team_type"].isna(), ["game_id", "team_id", "team_abv"]]
            .head(10)
            .to_dict(orient="records"),
        )

    return df


def attach_phase_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Attach normalized phase fields from game_week."""
    df = df.copy()
    phase_rows = df["game_week"].apply(normalize_game_week).apply(pd.Series)
    return pd.concat([df, phase_rows], axis=1)


def attach_registry_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Attach authoritative metadata from metric_registry.py.

    game_metrics_flat may contain legacy parser metrics that are not part of
    the approved registry. Those rows are excluded from the cleaned fact table.

    This keeps metric_registry.py as the source of truth without requiring us
    to mutate or clean the legacy source table first.
    """
    df = df.copy()

    source_metrics = set(df["metric"].dropna().unique())
    registry_metrics = set(METRIC_REGISTRY.keys())

    unregistered_source_metrics = sorted(source_metrics - registry_metrics)
    if unregistered_source_metrics:
        excluded_count = int(df[~df["metric"].isin(registry_metrics)].shape[0])
        log_event(
            "warning",
            "unregistered_source_metrics_excluded",
            metric_count=len(unregistered_source_metrics),
            row_count=excluded_count,
            metrics=unregistered_source_metrics,
        )
        df = df[df["metric"].isin(registry_metrics)].copy()

    if df.empty:
        raise ValueError(
            "No rows remain after filtering game_metrics_flat to registered metrics."
        )

    remaining_source_metrics = set(df["metric"].dropna().unique())

    meta_rows: List[Dict[str, Any]] = []
    for metric in sorted(remaining_source_metrics):
        meta = get_metric_meta(metric)

        # Keep lens_tags as an actual Python list so BigQuery can load it into
        # REPEATED STRING when using the existing table schema.
        if not isinstance(meta.get("lens_tags"), list):
            raise ValueError(f"{metric} lens_tags must be a list from metric_registry.py")

        meta_rows.append({"metric": metric, **meta})

    meta_df = pd.DataFrame(meta_rows)
    df = df.merge(meta_df, on="metric", how="left", validate="many_to_one")

    return df


def dedupe_fact_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure one row per season/game/team/metric.

    The legacy parser may emit duplicate rows for a metric. This keeps behavior
    practical while still producing a clean fact table grain.
    """
    df = df.copy()
    grain = ["season", "game_id", "team_id", "metric"]
    duplicate_mask = df.duplicated(subset=grain, keep=False)

    if duplicate_mask.any():
        duplicate_count = int(duplicate_mask.sum())
        log_event(
            "warning",
            "duplicate_fact_grain_rows_seen",
            rows=duplicate_count,
            keep=DEDUP_KEEP,
            sample=df.loc[duplicate_mask, grain + ["team_abv", "value"]]
            .sort_values(grain)
            .head(25)
            .to_dict(orient="records"),
        )
        df = df.drop_duplicates(subset=grain, keep=DEDUP_KEEP)

    return df


def validate_fact_df(df: pd.DataFrame, season: str) -> None:
    """Validate the cleaned fact DataFrame before writing to BigQuery."""
    if df.empty:
        raise ValueError(f"No fact rows produced for season={season}")

    required_not_null = [
        "season",
        "game_id",
        "game_date",
        "team_id",
        "team_abv",
        "metric",
        "value",
        "label",
        "definition",
        "category",
        "core_area",
        "comparison_direction",
        "raw_or_derived",
        "aggregation_method",
        "format",
        "decimals",
        "notes",
        "ranking_usage",
        "signal_strength",
        "edge_language_allowed",
        "include_in_core_area_advantage",
        "confidence_eligible",
        "data_quality_status",
    ]

    null_counts = df[required_not_null].isna().sum()
    bad_nulls = null_counts[null_counts > 0]
    if not bad_nulls.empty:
        sample = df[df[required_not_null].isna().any(axis=1)].head(20)
        log_event(
            "error",
            "fact_rows_missing_required_values",
            null_counts=bad_nulls.to_dict(),
            sample=sample.to_dict(orient="records"),
        )
        raise ValueError(f"Fact rows have nulls in required fields: {bad_nulls.to_dict()}")

    bad_lens_tags = df[
        ~df["lens_tags"].apply(lambda value: isinstance(value, list))
    ]
    if not bad_lens_tags.empty:
        log_event(
            "error",
            "fact_rows_invalid_lens_tags",
            rows=len(bad_lens_tags),
            sample=bad_lens_tags[["metric", "lens_tags"]]
            .head(20)
            .to_dict(orient="records"),
        )
        raise ValueError("Fact rows have invalid lens_tags values; expected list.")

    grain = ["season", "game_id", "team_id", "metric"]
    dupes = df.duplicated(subset=grain, keep=False)
    if dupes.any():
        sample = df.loc[dupes, grain + ["team_abv", "value"]].head(20)
        log_event(
            "error",
            "fact_grain_duplicates_after_dedupe",
            rows=int(dupes.sum()),
            sample=sample.to_dict(orient="records"),
        )
        raise ValueError("Duplicate fact-table grain remains after dedupe.")

    unknown_phase_count = int((df["season_phase"] == "unknown").sum())
    if unknown_phase_count:
        log_event(
            "warning",
            "unknown_game_week_phase_seen",
            rows=unknown_phase_count,
            game_weeks=sorted(
                df.loc[df["season_phase"] == "unknown", "game_week"]
                .dropna()
                .unique()
                .tolist()
            ),
        )

    log_event("info", "fact_df_validation_passed", season=season, rows=len(df))


def build_fact_dataframe(client: bigquery.Client, season: str) -> pd.DataFrame:
    """Build cleaned fact rows for one NFL season."""
    validate_metric_registry()

    df = load_game_metric_rows(client, season)
    if df.empty:
        raise ValueError(f"No source rows found for season={season}")

    df = attach_team_type(df)
    df = attach_phase_fields(df)
    df = attach_registry_metadata(df)
    df = dedupe_fact_rows(df)

    df["created_at"] = datetime.now(timezone.utc)

    # Drop schedule helper columns that are useful during transform but not
    # needed in the output fact table.
    for helper_col in [
        "game_status",
        "home_team_id",
        "away_team_id",
        "home_team_abv",
        "away_team_abv",
    ]:
        if helper_col in df.columns:
            df = df.drop(columns=[helper_col])

    # Enforce stable output ordering.
    missing_output_columns = [col for col in OUTPUT_COLUMNS if col not in df.columns]
    if missing_output_columns:
        raise ValueError(f"Missing output columns before final ordering: {missing_output_columns}")

    df = df[OUTPUT_COLUMNS]

    validate_fact_df(df, season)
    return df


# -----------------------------------------------------------------------------
# Write
# -----------------------------------------------------------------------------

def _resolve_write_disposition(if_exists: str) -> str:
    """Map user-facing if_exists values to BigQuery write dispositions."""
    if if_exists == "replace":
        return bigquery.WriteDisposition.WRITE_TRUNCATE
    if if_exists == "append":
        return bigquery.WriteDisposition.WRITE_APPEND
    if if_exists == "fail":
        return bigquery.WriteDisposition.WRITE_EMPTY
    raise ValueError(f"Unsupported if_exists value: {if_exists}")


def write_fact_table(df: pd.DataFrame, season: str, if_exists: str = "replace") -> str:
    """Write the cleaned fact DataFrame to BigQuery.

    The destination table should already exist with the explicit schema created by
    recreate_gamelens_metric_tables.py, including lens_tags as REPEATED STRING.
    """
    destination_table = OUTPUT_TABLE_TEMPLATE.format(season=season)
    table_id = f"{PROJECT}.{destination_table}"
    write_disposition = _resolve_write_disposition(if_exists)

    log_event(
        "info",
        "writing_game_team_metric_facts",
        table=destination_table,
        rows=len(df),
        if_exists=if_exists,
        write_disposition=str(write_disposition),
    )

    client = bigquery.Client(project=PROJECT)

    try:
        table = client.get_table(table_id)
    except NotFound as exc:
        raise RuntimeError(
            f"Destination table does not exist: {table_id}. "
            "Run recreate_gamelens_metric_tables.py first so lens_tags is created "
            "as REPEATED STRING."
        ) from exc

    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        write_disposition=write_disposition,
    )

    load_job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
    )
    load_job.result()

    log_event(
        "info",
        "game_team_metric_facts_written",
        table=destination_table,
        rows=len(df),
    )

    return table_id


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

def run_build_game_team_metric_facts(
    season: str = "2025",
    if_exists: str = "replace",
    write: bool = True,
) -> pd.DataFrame:
    """Build and optionally write Analytics.game_team_metric_facts_{season}."""
    setup_logging()
    log_event("info", "game_team_metric_facts_job_started", season=season, write=write)

    client = bigquery.Client(project=PROJECT)
    fact_df = build_fact_dataframe(client, season)

    if write:
        full_table_name = write_fact_table(fact_df, season, if_exists=if_exists)
        log_event(
            "info",
            "game_team_metric_facts_job_complete",
            season=season,
            table=full_table_name,
            rows=len(fact_df),
        )
    else:
        log_event(
            "info",
            "game_team_metric_facts_dry_run_complete",
            season=season,
            rows=len(fact_df),
        )

    return fact_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build cleaned GameLens game/team/metric fact table for one season."
    )
    parser.add_argument(
        "--season",
        default="2025",
        help="NFL season to build, for example: 2025 or 2026.",
    )
    parser.add_argument(
        "--if-exists",
        default="replace",
        choices=["fail", "replace", "append"],
        help=(
            "Write behavior for BigQuery load job. "
            "replace=WRITE_TRUNCATE, append=WRITE_APPEND, fail=WRITE_EMPTY."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate the DataFrame without writing to BigQuery.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_build_game_team_metric_facts(
        season=str(args.season),
        if_exists=args.if_exists,
        write=not args.dry_run,
    )
