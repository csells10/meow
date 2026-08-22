"""
Build GameLens windowed team metrics.

Purpose
-------
Read Analytics.game_team_metric_facts_{season}, build phase-aware and rolling
team metric windows, recalculate derived rates from summed ingredients, and
write Analytics.team_metrics_windowed_{season}.

This script does not change /game and does not re-call the external NFL API.

Important schema note
---------------------
The destination table should already exist with the explicit schema created by:

    recreate_gamelens_metric_tables.py

That schema includes lens_tags as REPEATED STRING. This builder uses the
existing BigQuery table schema during load so repeated fields are preserved.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from analytics.metric_registry import (  # type: ignore
    METRIC_REGISTRY,
    get_metric_config,
    get_metric_meta,
    validate_metric_registry,
)
from utils.logging_setup import log_event, setup_logging
from runtime_config import load_runtime_config


RUNTIME_CONFIG = load_runtime_config()
PROJECT = RUNTIME_CONFIG.project_id
SOURCE_TABLE_TEMPLATE = RUNTIME_CONFIG.analytics_object(
    "game_team_metric_facts_{season}"
)
OUTPUT_TABLE_TEMPLATE = RUNTIME_CONFIG.analytics_object(
    "team_metrics_windowed_{season}"
)

OUTPUT_VALUE_DECIMALS = 6

WINDOW_TYPES = [
    "regular_season_to_date",
    "regular_plus_postseason_to_date",
    "last_3_games",
    "last_7_games",
    "preseason_to_date",
]

OUTPUT_COLUMNS = [
    "season",
    "team_id",
    "team_abv",
    "data_date",
    "window_type",
    "games_in_window",
    "window_start_date",
    "window_end_date",
    "latest_included_game_id",
    "latest_included_global_week_order",
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
# BigQuery load
# -----------------------------------------------------------------------------

def load_fact_rows(client: bigquery.Client, season: str) -> pd.DataFrame:
    """Load cleaned game/team/metric facts for one season.

    The source fact table may contain rich metadata, but this builder only needs
    the game/team/window fields and metric values. Metadata is reattached from
    the current metric_registry.py so the windowed table always reflects the
    authoritative registry.
    """
    source_table = SOURCE_TABLE_TEMPLATE.format(season=season)

    query = f"""
        SELECT
            CAST(season AS STRING) AS season,
            game_id,
            DATE(game_date) AS game_date,
            game_week,
            season_phase,
            phase_week,
            global_week_order,
            team_id,
            team_abv,
            team_type,
            metric,
            SAFE_CAST(value AS FLOAT64) AS value
        FROM `{PROJECT}.{source_table}`
        WHERE CAST(season AS STRING) = @season
          AND metric IS NOT NULL
          AND game_id IS NOT NULL
          AND team_id IS NOT NULL
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("season", "STRING", season),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe(
        create_bqstorage_client=False
    )
    log_event(
        "info",
        "windowed_source_fact_rows_loaded",
        season=season,
        table=source_table,
        rows=len(df),
    )
    return df


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _to_python_date(value: Any) -> Any:
    """Return a date-like value that BigQuery can serialize cleanly."""
    if pd.isna(value):
        return None
    if hasattr(value, "date"):
        return value.date()
    return value


def _safe_number(value: Any) -> Optional[float]:
    """Convert NaN-like values to None, numeric values to float."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return float(value)


def _round_value(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), OUTPUT_VALUE_DECIMALS)


def dedupe_fact_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Guard against duplicate source fact rows before pivoting."""
    df = df.copy()
    grain = ["season", "game_id", "team_id", "metric"]
    duplicate_mask = df.duplicated(subset=grain, keep=False)

    if duplicate_mask.any():
        log_event(
            "warning",
            "windowed_source_fact_duplicates_seen",
            rows=int(duplicate_mask.sum()),
            sample=df.loc[duplicate_mask, grain + ["team_abv", "value"]]
            .sort_values(grain)
            .head(25)
            .to_dict(orient="records"),
        )
        df = df.drop_duplicates(subset=grain, keep="last")

    return df


def pivot_fact_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot long metric facts to one row per team/game."""
    df = df.copy()

    # Optional descriptive fields must not be pivot keys. Pandas drops rows
    # whose pivot index contains nulls, which erased valid unnumbered games.
    index_cols = [
        "season",
        "team_id",
        "team_abv",
        "game_id",
        "game_date",
        "season_phase",
        "global_week_order",
    ]

    df["global_week_order"] = pd.to_numeric(
        df["global_week_order"], errors="coerce"
    )

    null_index_counts = df[index_cols].isna().sum()
    bad_index_nulls = null_index_counts[null_index_counts > 0]
    if not bad_index_nulls.empty:
        sample = df[df[index_cols].isna().any(axis=1)].head(20)
        log_event(
            "error",
            "windowed_pivot_required_keys_missing",
            null_counts=bad_index_nulls.to_dict(),
            sample=sample.to_dict(orient="records"),
        )
        raise ValueError(
            "Windowed pivot rows have nulls in required keys: "
            f"{bad_index_nulls.to_dict()}"
        )

    expected_rows = len(
        df[["season", "team_id", "game_id"]].drop_duplicates()
    )

    pivot = (
        df.pivot_table(
            index=index_cols,
            columns="metric",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )

    # Remove the columns index name left by pivot_table.
    pivot.columns.name = None

    pivot["game_date"] = pd.to_datetime(pivot["game_date"])

    if len(pivot) != expected_rows:
        raise ValueError(
            "Windowed pivot lost team/game rows: "
            f"expected={expected_rows}, actual={len(pivot)}"
        )

    log_event(
        "info",
        "windowed_pivot_created",
        rows=len(pivot),
        columns=len(pivot.columns),
    )
    return pivot


def get_window_subset(
    team_games: pd.DataFrame,
    current_row: pd.Series,
    window_type: str,
) -> pd.DataFrame:
    """Return the games included in a specific window as of current_row."""
    current_order = current_row["global_week_order"]
    current_phase = current_row["season_phase"]

    if pd.isna(current_order):
        return team_games.iloc[0:0]

    prior_or_current = team_games[team_games["global_week_order"] <= current_order]

    if window_type == "preseason_to_date":
        if current_phase != "preseason":
            return team_games.iloc[0:0]
        return prior_or_current[prior_or_current["season_phase"] == "preseason"]

    if window_type == "regular_season_to_date":
        if current_phase != "regular_season":
            return team_games.iloc[0:0]
        return prior_or_current[prior_or_current["season_phase"] == "regular_season"]

    meaningful = prior_or_current[
        prior_or_current["season_phase"].isin(["regular_season", "postseason"])
    ]

    if window_type == "regular_plus_postseason_to_date":
        if current_phase not in {"regular_season", "postseason"}:
            return team_games.iloc[0:0]
        return meaningful

    if window_type == "last_3_games":
        if current_phase not in {"regular_season", "postseason"}:
            return team_games.iloc[0:0]
        return meaningful.tail(3)

    if window_type == "last_7_games":
        if current_phase not in {"regular_season", "postseason"}:
            return team_games.iloc[0:0]
        return meaningful.tail(7)

    raise ValueError(f"Unsupported window_type: {window_type}")


def calculate_metric_value(
    metric: str,
    cfg: Dict[str, Any],
    subset: pd.DataFrame,
    metric_sums: pd.Series,
) -> Optional[float]:
    """Calculate one metric for a window.

    Important rules:
    - ratio_from_sums metrics are recalculated from summed numerator/denominator
      ingredients. They are not averaged from game-level percentages.
    - per_game_from_sum metrics divide the summed numerator by the actual number
      of games in the current window subset. This keeps last_3_games,
      last_7_games, and early-season partial windows accurate.
    """
    aggregation_method = cfg["aggregation_method"]

    if aggregation_method == "ratio_from_sums":
        numerator_metric = cfg.get("numerator")
        denominator_metric = cfg.get("denominator")

        if not numerator_metric or not denominator_metric:
            raise ValueError(
                f"{metric} uses ratio_from_sums but is missing numerator/denominator"
            )

        numerator = _safe_number(metric_sums.get(numerator_metric))
        denominator = _safe_number(metric_sums.get(denominator_metric))

        if denominator is None or denominator == 0:
            return None

        if numerator is None:
            numerator = 0.0

        return numerator / denominator

    if aggregation_method == "per_game_from_sum":
        numerator_metric = cfg.get("numerator") or metric

        if not numerator_metric:
            raise ValueError(
                f"{metric} uses per_game_from_sum but is missing numerator"
            )

        numerator = _safe_number(metric_sums.get(numerator_metric))

        if numerator is None:
            return None

        games_in_window = len(subset)
        if games_in_window == 0:
            return None

        return numerator / games_in_window

    if aggregation_method == "sum":
        return _safe_number(metric_sums.get(metric))

    if aggregation_method == "mean_contextual":
        if metric not in subset.columns:
            return None
        value = subset[metric].mean(skipna=True)
        return _safe_number(value)

    if aggregation_method == "latest":
        if metric not in subset.columns:
            return None
        non_null = subset[metric].dropna()
        if non_null.empty:
            return None
        return _safe_number(non_null.iloc[-1])

    raise ValueError(f"Unsupported aggregation_method for {metric}: {aggregation_method}")


def _metadata_payload(metric: str) -> Dict[str, Any]:
    """Return full registry metadata for one metric, with basic lens_tags guard."""
    meta = get_metric_meta(metric)

    if not isinstance(meta.get("lens_tags"), list):
        raise ValueError(f"{metric} lens_tags must be a list from metric_registry.py")

    return meta


def build_snapshot_rows(
    team_id: str,
    team_abv: str,
    season: str,
    window_type: str,
    subset: pd.DataFrame,
    created_at: datetime,
) -> List[Dict[str, Any]]:
    """Build all metric rows for one team/window snapshot."""
    if subset.empty:
        return []

    subset = subset.sort_values(["global_week_order", "game_date", "game_id"])

    metric_columns = [metric for metric in METRIC_REGISTRY if metric in subset.columns]
    metric_sums = subset[metric_columns].sum(skipna=True, min_count=1)

    latest_row = subset.iloc[-1]
    window_start_date = _to_python_date(subset["game_date"].min())
    window_end_date = _to_python_date(subset["game_date"].max())
    data_date = window_end_date

    rows: List[Dict[str, Any]] = []

    for metric, cfg in METRIC_REGISTRY.items():
        value = calculate_metric_value(metric, cfg, subset, metric_sums)
        meta = _metadata_payload(metric)

        rows.append(
            {
                "season": season,
                "team_id": team_id,
                "team_abv": team_abv,
                "data_date": data_date,
                "window_type": window_type,
                "games_in_window": int(len(subset)),
                "window_start_date": window_start_date,
                "window_end_date": window_end_date,
                "latest_included_game_id": latest_row["game_id"],
                "latest_included_global_week_order": (
                    None
                    if pd.isna(latest_row["global_week_order"])
                    else int(latest_row["global_week_order"])
                ),
                "metric": metric,
                "value": _round_value(value),

                # Registry metadata
                "label": meta["label"],
                "definition": meta["definition"],
                "category": meta["category"],
                "core_area": meta["core_area"],
                "comparison_direction": meta["comparison_direction"],
                "higher_is_better": meta["higher_is_better"],
                "raw_or_derived": meta["raw_or_derived"],
                "aggregation_method": meta["aggregation_method"],
                "numerator": meta["numerator"],
                "denominator": meta["denominator"],
                "format": meta["format"],
                "decimals": meta["decimals"],
                "notes": meta["notes"],
                "ranking_usage": meta["ranking_usage"],
                "signal_strength": meta["signal_strength"],
                "edge_language_allowed": meta["edge_language_allowed"],
                "include_in_core_area_advantage": meta["include_in_core_area_advantage"],
                "confidence_eligible": meta["confidence_eligible"],
                "data_quality_status": meta["data_quality_status"],
                "lens_tags": meta["lens_tags"],

                "created_at": created_at,
            }
        )

    return rows


# -----------------------------------------------------------------------------
# Build
# -----------------------------------------------------------------------------

def build_windowed_dataframe(client: bigquery.Client, season: str) -> pd.DataFrame:
    """Build all windowed metric rows for one season."""
    validate_metric_registry()

    facts = load_fact_rows(client, season)
    if facts.empty:
        raise ValueError(f"No cleaned fact rows found for season={season}")

    facts = dedupe_fact_rows(facts)
    pivot = pivot_fact_rows(facts)

    created_at = datetime.now(timezone.utc)
    results: List[Dict[str, Any]] = []

    for team_id, team_games in pivot.groupby("team_id"):
        team_games = team_games.sort_values(
            ["global_week_order", "game_date", "game_id"],
            na_position="last",
        ).reset_index(drop=True)

        team_abv = str(team_games["team_abv"].iloc[0])

        for _, current_row in team_games.iterrows():
            for window_type in WINDOW_TYPES:
                subset = get_window_subset(team_games, current_row, window_type)
                if subset.empty:
                    continue

                results.extend(
                    build_snapshot_rows(
                        team_id=str(team_id),
                        team_abv=team_abv,
                        season=season,
                        window_type=window_type,
                        subset=subset,
                        created_at=created_at,
                    )
                )

    result_df = pd.DataFrame(results)

    if result_df.empty:
        raise ValueError(f"No windowed rows produced for season={season}")

    missing_output_columns = [col for col in OUTPUT_COLUMNS if col not in result_df.columns]
    if missing_output_columns:
        raise ValueError(
            f"Missing output columns before final ordering: {missing_output_columns}"
        )

    result_df = result_df[OUTPUT_COLUMNS]
    validate_windowed_df(result_df, season)

    log_event(
        "info",
        "windowed_dataframe_built",
        season=season,
        rows=len(result_df),
        windows=result_df["window_type"].nunique(),
    )

    return result_df


# -----------------------------------------------------------------------------
# Validate
# -----------------------------------------------------------------------------

def validate_windowed_df(df: pd.DataFrame, season: str) -> None:
    """Validate windowed rows before writing to BigQuery."""
    required_not_null = [
        "season",
        "team_id",
        "team_abv",
        "data_date",
        "window_type",
        "games_in_window",
        "window_start_date",
        "window_end_date",
        "latest_included_game_id",
        "metric",
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
            "windowed_rows_missing_required_values",
            null_counts=bad_nulls.to_dict(),
            sample=sample.to_dict(orient="records"),
        )
        raise ValueError(
            f"Windowed rows have nulls in required fields: {bad_nulls.to_dict()}"
        )

    bad_lens_tags = df[
        ~df["lens_tags"].apply(lambda value: isinstance(value, list))
    ]
    if not bad_lens_tags.empty:
        log_event(
            "error",
            "windowed_rows_invalid_lens_tags",
            rows=len(bad_lens_tags),
            sample=bad_lens_tags[["metric", "lens_tags"]]
            .head(20)
            .to_dict(orient="records"),
        )
        raise ValueError("Windowed rows have invalid lens_tags values; expected list.")

    grain = ["season", "team_id", "data_date", "window_type", "metric"]
    dupes = df.duplicated(subset=grain, keep=False)
    if dupes.any():
        sample = df.loc[dupes, grain + ["team_abv", "value"]].head(20)
        log_event(
            "error",
            "windowed_grain_duplicates_seen",
            rows=int(dupes.sum()),
            sample=sample.to_dict(orient="records"),
        )
        raise ValueError("Duplicate windowed-table grain rows found.")

    last_3_bad = df[
        (df["window_type"] == "last_3_games") & (df["games_in_window"] > 3)
    ]
    if not last_3_bad.empty:
        raise ValueError("last_3_games contains rows with games_in_window > 3")

    last_7_bad = df[
        (df["window_type"] == "last_7_games") & (df["games_in_window"] > 7)
    ]
    if not last_7_bad.empty:
        raise ValueError("last_7_games contains rows with games_in_window > 7")

    summary_df = (
        df.groupby("window_type")
        .agg(
            rows=("metric", "count"),
            teams=("team_id", "nunique"),
            first_date=("data_date", "min"),
            last_date=("data_date", "max"),
        )
        .reset_index()
    )

    # Convert date-like values to strings before logging.
    # The structured JSON logger cannot serialize Python date objects directly.
    summary_df["first_date"] = summary_df["first_date"].astype(str)
    summary_df["last_date"] = summary_df["last_date"].astype(str)
    summary = summary_df.to_dict(orient="records")

    log_event(
        "info",
        "windowed_df_validation_passed",
        season=season,
        rows=len(df),
        summary=summary,
    )


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


def write_windowed_table(df: pd.DataFrame, season: str, if_exists: str = "replace") -> str:
    """Write the windowed metric DataFrame to BigQuery.

    The destination table should already exist with the explicit schema created by
    recreate_gamelens_metric_tables.py, including lens_tags as REPEATED STRING.
    """
    destination_table = OUTPUT_TABLE_TEMPLATE.format(season=season)
    table_id = f"{PROJECT}.{destination_table}"
    write_disposition = _resolve_write_disposition(if_exists)

    log_event(
        "info",
        "writing_windowed_metrics",
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
        "windowed_metrics_written",
        table=destination_table,
        rows=len(df),
    )

    return table_id


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

def run_build_windowed_metrics(
    season: str = "2025",
    if_exists: str = "replace",
    write: bool = True,
) -> pd.DataFrame:
    """Build and optionally write Analytics.team_metrics_windowed_{season}."""
    setup_logging()
    log_event("info", "windowed_metrics_job_started", season=season, write=write)

    client = bigquery.Client(project=PROJECT)
    windowed_df = build_windowed_dataframe(client, season)

    if write:
        full_table_name = write_windowed_table(windowed_df, season, if_exists=if_exists)
        log_event(
            "info",
            "windowed_metrics_job_complete",
            season=season,
            table=full_table_name,
            rows=len(windowed_df),
        )
    else:
        log_event(
            "info",
            "windowed_metrics_dry_run_complete",
            season=season,
            rows=len(windowed_df),
        )

    return windowed_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build GameLens windowed team metrics for one season."
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
    run_build_windowed_metrics(
        season=str(args.season),
        if_exists=args.if_exists,
        write=not args.dry_run,
    )
