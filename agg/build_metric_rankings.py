"""
Build GameLens as-of team metric rankings.

Purpose
-------
Read Analytics.team_metrics_windowed_{season}, build league-wide as-of rankings
for each:

    season + as_of_date + window_type + metric + team_id

and write:

    Analytics.team_metric_rankings_{season}

This table is a backend/data-layer product asset. It does NOT change /game and
does NOT force winner predictions.

Why as_of_date exists
---------------------
The source windowed table stores team snapshots by each team's own latest game
date. If rankings are built only on exact data_date matches, a date like the
final regular-season Sunday may rank only the teams that played that day.

This builder instead uses carry-forward logic:

    For each as_of_date + window_type + metric:
        For each team:
            use the latest source_data_date <= as_of_date

That lets the ranking table answer the product question:

    "As of this date, where did every available team rank?"

rather than:

    "Which teams happened to have rows dated exactly this date?"

Design decisions baked in
-------------------------
1. Grain:
   season + as_of_date + window_type + metric + team_id

2. Source:
   Analytics.team_metrics_windowed_{season}

3. Date fields:
   - as_of_date: ranking date
   - source_data_date: latest team metric snapshot used for that team
   - data_lag_days: days between as_of_date and source_data_date

4. Exclude:
   ranking_usage='exclude' or data_quality_status='exclude' metrics are not ranked.

5. Edge metrics:
   ranking_usage='edge' metrics are ranked using comparison_direction:
   - higher => higher value gets better rank
   - lower  => lower value gets better rank

6. Context metrics:
   ranking_usage='context_only' metrics are ranked by raw value high-to-low.
   This rank describes distribution/volume/style only. It must not be treated as
   better/worse edge language.

7. No conference/division yet:
   First version is league-wide only. Conference/division can be joined later.

8. Tiers:
   Edge metrics use edge-language tiers:
   - elite
   - strong
   - average
   - weak
   - poor

   Context metrics use distribution tiers:
   - very_high
   - high
   - typical
   - low
   - very_low

9. lens_tags:
   Preserved as REPEATED STRING using the existing destination table schema.

10. Ties:
    league_rank uses competition/min rank. If two teams tie for rank 2, both get
    rank 2 and the next distinct rank is 4.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from utils.logging_setup import log_event, setup_logging
from analytics.metric_registry import validate_metric_registry  # type: ignore
from runtime_config import load_runtime_config


RUNTIME_CONFIG = load_runtime_config()
PROJECT = RUNTIME_CONFIG.project_id
SOURCE_TABLE_TEMPLATE = RUNTIME_CONFIG.analytics_object(
    "team_metrics_windowed_{season}"
)
OUTPUT_TABLE_TEMPLATE = RUNTIME_CONFIG.analytics_object(
    "team_metric_rankings_{season}"
)

PERCENTILE_DECIMALS = 2

OUTPUT_COLUMNS = [
    "season",
    "as_of_date",
    "source_data_date",
    "data_lag_days",
    "window_type",
    "metric",
    "team_id",
    "team_abv",
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

    # Ranking fields
    "league_rank",
    "league_percentile",
    "tier",
    "tier_label",
    "teams_ranked",
    "ranking_kind",
    "rank_direction",
    "rank_interpretation",
    "rank_tie_method",

    "created_at",
]


# -----------------------------------------------------------------------------
# Schema
# -----------------------------------------------------------------------------

def metric_metadata_schema() -> List[bigquery.SchemaField]:
    """Metadata fields carried from analytics.metric_registry.py."""
    return [
        bigquery.SchemaField(
            "label",
            "STRING",
            mode="REQUIRED",
            description="Display label for the metric, sourced from metric_registry.py.",
        ),
        bigquery.SchemaField(
            "definition",
            "STRING",
            mode="REQUIRED",
            description="Plain-English definition of what the metric measures.",
        ),
        bigquery.SchemaField(
            "category",
            "STRING",
            mode="REQUIRED",
            description="Metric category used for product grouping.",
        ),
        bigquery.SchemaField(
            "core_area",
            "STRING",
            mode="REQUIRED",
            description="High-level matchup area the metric belongs to.",
        ),
        bigquery.SchemaField(
            "comparison_direction",
            "STRING",
            mode="REQUIRED",
            description="How values compare: higher, lower, or context.",
        ),
        bigquery.SchemaField(
            "higher_is_better",
            "BOOLEAN",
            mode="NULLABLE",
            description="True for higher-is-better metrics, False for lower-is-better metrics, NULL for context metrics.",
        ),
        bigquery.SchemaField(
            "raw_or_derived",
            "STRING",
            mode="REQUIRED",
            description="Whether the metric is raw, derived, or contextual.",
        ),
        bigquery.SchemaField(
            "aggregation_method",
            "STRING",
            mode="REQUIRED",
            description="How the metric aggregates across games, such as sum, ratio_from_sums, mean_contextual, or latest.",
        ),
        bigquery.SchemaField(
            "numerator",
            "STRING",
            mode="NULLABLE",
            description="Numerator metric for ratio_from_sums derived metrics. NULL for non-ratio metrics.",
        ),
        bigquery.SchemaField(
            "denominator",
            "STRING",
            mode="NULLABLE",
            description="Denominator metric for ratio_from_sums derived metrics. NULL for non-ratio metrics.",
        ),
        bigquery.SchemaField(
            "format",
            "STRING",
            mode="REQUIRED",
            description="Display formatting hint, such as integer, decimal, percent, or duration_minutes.",
        ),
        bigquery.SchemaField(
            "decimals",
            "INTEGER",
            mode="REQUIRED",
            description="Preferred number of decimal places for display formatting.",
        ),
        bigquery.SchemaField(
            "notes",
            "STRING",
            mode="REQUIRED",
            description="Metric interpretation notes and product guardrails from metric_registry.py.",
        ),
        bigquery.SchemaField(
            "ranking_usage",
            "STRING",
            mode="REQUIRED",
            description="How ranking/explanation layers may use the metric: edge, context_only, or exclude.",
        ),
        bigquery.SchemaField(
            "signal_strength",
            "STRING",
            mode="REQUIRED",
            description="How loudly the metric may speak when eligible: strong, supporting, context, or exclude.",
        ),
        bigquery.SchemaField(
            "edge_language_allowed",
            "BOOLEAN",
            mode="REQUIRED",
            description="Whether the product may use this metric for better/worse edge language.",
        ),
        bigquery.SchemaField(
            "include_in_core_area_advantage",
            "BOOLEAN",
            mode="REQUIRED",
            description="Whether this metric may contribute to Core Area Advantage scoring.",
        ),
        bigquery.SchemaField(
            "confidence_eligible",
            "BOOLEAN",
            mode="REQUIRED",
            description="Whether this metric may influence confidence/model-trust language.",
        ),
        bigquery.SchemaField(
            "data_quality_status",
            "STRING",
            mode="REQUIRED",
            description="Metric data-quality status: good, watch, or exclude.",
        ),
        bigquery.SchemaField(
            "lens_tags",
            "STRING",
            mode="REPEATED",
            description="Frontend/product grouping tags from metric_registry.py.",
        ),
    ]


def ranking_table_schema() -> List[bigquery.SchemaField]:
    """Schema for Analytics.team_metric_rankings_{season}."""
    return [
        bigquery.SchemaField(
            "season",
            "STRING",
            mode="REQUIRED",
            description="NFL season year, such as 2025.",
        ),
        bigquery.SchemaField(
            "as_of_date",
            "DATE",
            mode="REQUIRED",
            description="Ranking date. For each team, the ranking uses the latest source_data_date on or before this date.",
        ),
        bigquery.SchemaField(
            "source_data_date",
            "DATE",
            mode="REQUIRED",
            description="Actual team metric snapshot date used for this ranking row.",
        ),
        bigquery.SchemaField(
            "data_lag_days",
            "INTEGER",
            mode="REQUIRED",
            description="Number of days between as_of_date and source_data_date. Zero means the team updated on the ranking date.",
        ),
        bigquery.SchemaField(
            "window_type",
            "STRING",
            mode="REQUIRED",
            description="Window definition used for the metric, such as regular_season_to_date, last_3_games, last_7_games, preseason_to_date, or regular_plus_postseason_to_date.",
        ),
        bigquery.SchemaField(
            "metric",
            "STRING",
            mode="REQUIRED",
            description="Canonical metric key from metric_registry.py.",
        ),
        bigquery.SchemaField(
            "team_id",
            "STRING",
            mode="REQUIRED",
            description="Team identifier from the source data.",
        ),
        bigquery.SchemaField(
            "team_abv",
            "STRING",
            mode="REQUIRED",
            description="Team abbreviation, such as BUF, PHI, or DET.",
        ),
        bigquery.SchemaField(
            "value",
            "FLOAT",
            mode="REQUIRED",
            description="Windowed metric value being ranked.",
        ),
        *metric_metadata_schema(),
        bigquery.SchemaField(
            "league_rank",
            "INTEGER",
            mode="REQUIRED",
            description="League-wide rank for this metric/as-of date/window. Rank 1 is the best directional value for edge metrics or highest raw value for context metrics.",
        ),
        bigquery.SchemaField(
            "league_percentile",
            "FLOAT",
            mode="REQUIRED",
            description="League-relative percentile from 0 to 100. For edge metrics, higher is better. For context metrics, higher means higher raw-value distribution, not better team quality.",
        ),
        bigquery.SchemaField(
            "tier",
            "STRING",
            mode="REQUIRED",
            description="Rank band code. Edge metrics use elite/strong/average/weak/poor. Context metrics use very_high/high/typical/low/very_low.",
        ),
        bigquery.SchemaField(
            "tier_label",
            "STRING",
            mode="REQUIRED",
            description="Human-readable label for tier.",
        ),
        bigquery.SchemaField(
            "teams_ranked",
            "INTEGER",
            mode="REQUIRED",
            description="Number of teams included in this metric/as-of date/window ranking group.",
        ),
        bigquery.SchemaField(
            "ranking_kind",
            "STRING",
            mode="REQUIRED",
            description="Ranking type: edge for better/worse metrics, context for descriptive metrics.",
        ),
        bigquery.SchemaField(
            "rank_direction",
            "STRING",
            mode="REQUIRED",
            description="How the rank was calculated: higher_is_better, lower_is_better, or high_value_context.",
        ),
        bigquery.SchemaField(
            "rank_interpretation",
            "STRING",
            mode="REQUIRED",
            description="Plain-English guardrail explaining how this rank should be interpreted.",
        ),
        bigquery.SchemaField(
            "rank_tie_method",
            "STRING",
            mode="REQUIRED",
            description="Tie handling method used for league_rank. Current value: competition_min_rank.",
        ),
        bigquery.SchemaField(
            "created_at",
            "TIMESTAMP",
            mode="REQUIRED",
            description="Timestamp when this ranking row was generated.",
        ),
    ]


# -----------------------------------------------------------------------------
# BigQuery table helpers
# -----------------------------------------------------------------------------

def full_table_id(table_name: str) -> str:
    return f"{PROJECT}.{table_name}"


def ensure_ranking_table(
    client: bigquery.Client,
    *,
    season: str,
    recreate_table: bool = False,
) -> str:
    """Create the output ranking table if needed, optionally recreating it."""
    table_name = OUTPUT_TABLE_TEMPLATE.format(season=season)
    table_id = full_table_id(table_name)

    if recreate_table:
        client.delete_table(table_id, not_found_ok=True)
        log_event("info", "ranking_table_deleted_for_recreate", table=table_name)

    try:
        client.get_table(table_id)
        log_event("info", "ranking_table_exists", table=table_name)
        return table_id
    except NotFound:
        pass

    table = bigquery.Table(table_id, schema=ranking_table_schema())
    table.description = (
        "GameLens league-wide as-of team metric rankings. Built from "
        f"{SOURCE_TABLE_TEMPLATE.format(season=season)}. Rankings are league-wide only; "
        "conference/division context is intentionally excluded for v1. "
        "Rows use carry-forward logic so each as_of_date ranks every team with an available prior snapshot."
    )
    table.labels = {
        "app": "gamelens",
        "domain": "nfl",
        "managed_by": "python",
    }

    created = client.create_table(table)
    log_event("info", "ranking_table_created", table=table_name)
    return created.full_table_id.replace(":", ".")


# -----------------------------------------------------------------------------
# Source load
# -----------------------------------------------------------------------------

def load_windowed_rows(client: bigquery.Client, season: str) -> pd.DataFrame:
    """Load rankable windowed rows for one season."""
    source_table = SOURCE_TABLE_TEMPLATE.format(season=season)

    query = f"""
        SELECT
            CAST(season AS STRING) AS season,
            DATE(data_date) AS source_data_date,
            window_type,
            metric,
            CAST(team_id AS STRING) AS team_id,
            team_abv,
            SAFE_CAST(value AS FLOAT64) AS value,

            label,
            definition,
            category,
            core_area,
            comparison_direction,
            higher_is_better,
            raw_or_derived,
            aggregation_method,
            numerator,
            denominator,
            format,
            decimals,
            notes,
            ranking_usage,
            signal_strength,
            edge_language_allowed,
            include_in_core_area_advantage,
            confidence_eligible,
            data_quality_status,
            lens_tags
        FROM `{PROJECT}.{source_table}`
        WHERE CAST(season AS STRING) = @season
          AND value IS NOT NULL
          AND ranking_usage IN ('edge', 'context_only')
          AND data_quality_status != 'exclude'
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("season", "STRING", season),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    log_event(
        "info",
        "rankable_windowed_rows_loaded",
        season=season,
        source_table=source_table,
        rows=len(df),
    )

    return df


# -----------------------------------------------------------------------------
# Ranking logic
# -----------------------------------------------------------------------------

def normalize_lens_tags(value: Any) -> List[str]:
    """Normalize BigQuery repeated values into a plain Python list[str]."""
    if value is None:
        return []

    try:
        if pd.isna(value):
            return []
    except (TypeError, ValueError):
        pass

    if isinstance(value, list):
        return [str(item) for item in value]

    if isinstance(value, tuple):
        return [str(item) for item in value]

    if hasattr(value, "tolist"):
        converted = value.tolist()
        if isinstance(converted, list):
            return [str(item) for item in converted]
        return [str(converted)]

    return [str(value)]


def determine_rank_direction(row: pd.Series) -> str:
    """Determine how the rank should be calculated."""
    ranking_usage = row["ranking_usage"]
    comparison_direction = row["comparison_direction"]

    if ranking_usage == "context_only":
        return "high_value_context"

    if comparison_direction == "higher":
        return "higher_is_better"

    if comparison_direction == "lower":
        return "lower_is_better"

    raise ValueError(
        "Invalid ranking configuration. Edge rankings require comparison_direction "
        f"higher/lower. metric={row['metric']}, ranking_usage={ranking_usage}, "
        f"comparison_direction={comparison_direction}"
    )


def build_rank_interpretation(rank_direction: str, ranking_usage: str) -> str:
    """Return product-safe interpretation text for a ranking row."""
    if ranking_usage == "context_only":
        return (
            "Context ranking only. Higher percentile means a higher raw value relative "
            "to the league, not a better team edge."
        )

    if rank_direction == "higher_is_better":
        return "Directional edge ranking. Higher metric values rank better."

    if rank_direction == "lower_is_better":
        return "Directional edge ranking. Lower metric values rank better."

    return "Ranking interpretation unavailable."


def tier_for_percentile(percentile: float, ranking_usage: str) -> Dict[str, str]:
    """Map percentile into product-friendly tier fields."""
    if ranking_usage == "context_only":
        if percentile >= 90:
            return {"tier": "very_high", "tier_label": "Very high"}
        if percentile >= 70:
            return {"tier": "high", "tier_label": "High"}
        if percentile > 30:
            return {"tier": "typical", "tier_label": "Typical"}
        if percentile > 10:
            return {"tier": "low", "tier_label": "Low"}
        return {"tier": "very_low", "tier_label": "Very low"}

    if percentile >= 90:
        return {"tier": "elite", "tier_label": "Elite"}
    if percentile >= 70:
        return {"tier": "strong", "tier_label": "Strong"}
    if percentile > 30:
        return {"tier": "average", "tier_label": "Average"}
    if percentile > 10:
        return {"tier": "weak", "tier_label": "Weak"}
    return {"tier": "poor", "tier_label": "Poor"}


def add_ranks_for_as_of_group(group: pd.DataFrame) -> pd.DataFrame:
    """Rank one season/as_of_date/window/metric group."""
    group = group.copy()

    first = group.iloc[0]
    rank_direction = determine_rank_direction(first)
    ranking_usage = str(first["ranking_usage"])

    if rank_direction == "lower_is_better":
        group["_rank_value"] = -group["value"].astype(float)
    else:
        # higher_is_better and high_value_context both rank high raw values first.
        group["_rank_value"] = group["value"].astype(float)

    group["league_rank"] = (
        group["_rank_value"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    teams_ranked = int(len(group))
    group["teams_ranked"] = teams_ranked

    if teams_ranked <= 1:
        group["league_percentile"] = 100.0
    else:
        group["league_percentile"] = (
            (teams_ranked - group["league_rank"]) / (teams_ranked - 1) * 100
        ).round(PERCENTILE_DECIMALS)

    group["ranking_kind"] = "context" if ranking_usage == "context_only" else "edge"
    group["rank_direction"] = rank_direction
    group["rank_interpretation"] = build_rank_interpretation(
        rank_direction=rank_direction,
        ranking_usage=ranking_usage,
    )
    group["rank_tie_method"] = "competition_min_rank"

    tier_rows = group["league_percentile"].apply(
        lambda percentile: tier_for_percentile(float(percentile), ranking_usage)
    )
    tier_df = pd.DataFrame(tier_rows.tolist(), index=group.index)

    group["tier"] = tier_df["tier"]
    group["tier_label"] = tier_df["tier_label"]

    return group.drop(columns=["_rank_value"])


def build_as_of_snapshots(source_df: pd.DataFrame) -> pd.DataFrame:
    """Create carry-forward as-of snapshots before ranking.

    For each season/window_type/metric/as_of_date:
      - consider all rows with source_data_date <= as_of_date
      - keep the latest row per team
      - rank that carried-forward team set
    """
    source_df = source_df.copy()

    source_df["_source_data_date_ts"] = pd.to_datetime(source_df["source_data_date"])
    source_df = source_df.sort_values(
        ["season", "window_type", "metric", "team_id", "_source_data_date_ts"]
    )

    # Use all available dates per season/window as ranking dates so the table has
    # a stable as-of calendar within each window type. This avoids per-metric
    # date gaps when a metric has null values on a specific date.
    as_of_dates_by_window = {
        key: sorted(group["_source_data_date_ts"].dropna().unique())
        for key, group in source_df.groupby(["season", "window_type"], sort=False)
    }

    ranking_groups: List[pd.DataFrame] = []

    for (season, window_type, metric), metric_group in source_df.groupby(
        ["season", "window_type", "metric"],
        sort=False,
    ):
        as_of_dates = as_of_dates_by_window.get((season, window_type), [])

        if not as_of_dates:
            continue

        metric_group = metric_group.sort_values(["team_id", "_source_data_date_ts"])

        for as_of_ts in as_of_dates:
            eligible = metric_group[
                metric_group["_source_data_date_ts"] <= as_of_ts
            ]

            if eligible.empty:
                continue

            latest_per_team = (
                eligible.sort_values(["team_id", "_source_data_date_ts"])
                .drop_duplicates(subset=["team_id"], keep="last")
                .copy()
            )

            if latest_per_team.empty:
                continue

            latest_per_team["as_of_date"] = pd.Timestamp(as_of_ts).date()
            latest_per_team["source_data_date"] = latest_per_team[
                "_source_data_date_ts"
            ].dt.date
            latest_per_team["data_lag_days"] = (
                pd.Timestamp(as_of_ts) - latest_per_team["_source_data_date_ts"]
            ).dt.days.astype(int)

            ranked = add_ranks_for_as_of_group(latest_per_team)
            ranking_groups.append(ranked)

    if not ranking_groups:
        return pd.DataFrame()

    result = pd.concat(ranking_groups, ignore_index=True)
    result = result.drop(columns=["_source_data_date_ts"], errors="ignore")

    return result


def build_rankings_dataframe(client: bigquery.Client, season: str) -> pd.DataFrame:
    """Build league-wide as-of rankings for one season."""
    validate_metric_registry()

    source_df = load_windowed_rows(client, season)
    if source_df.empty:
        raise ValueError(f"No rankable windowed rows found for season={season}")

    source_df = source_df.copy()
    source_df["lens_tags"] = source_df["lens_tags"].apply(normalize_lens_tags)
    source_df["created_at"] = datetime.now(timezone.utc)

    ranked = build_as_of_snapshots(source_df)

    if ranked.empty:
        raise ValueError(f"No ranking rows produced for season={season}")

    missing_output_columns = [col for col in OUTPUT_COLUMNS if col not in ranked.columns]
    if missing_output_columns:
        raise ValueError(
            f"Missing output columns before final ordering: {missing_output_columns}"
        )

    ranked = ranked[OUTPUT_COLUMNS]

    validate_rankings_df(ranked, season)

    log_event(
        "info",
        "rankings_dataframe_built",
        season=season,
        rows=len(ranked),
        metrics=ranked["metric"].nunique(),
        windows=ranked["window_type"].nunique(),
        dates=ranked["as_of_date"].nunique(),
    )

    return ranked


# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------

def validate_rankings_df(df: pd.DataFrame, season: str) -> None:
    """Validate the ranking output before writing."""
    if df.empty:
        raise ValueError(f"No ranking rows produced for season={season}")

    required_not_null = [
        "season",
        "as_of_date",
        "source_data_date",
        "data_lag_days",
        "window_type",
        "metric",
        "team_id",
        "team_abv",
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
        "league_rank",
        "league_percentile",
        "tier",
        "tier_label",
        "teams_ranked",
        "ranking_kind",
        "rank_direction",
        "rank_interpretation",
        "rank_tie_method",
        "created_at",
    ]

    null_counts = df[required_not_null].isna().sum()
    bad_nulls = null_counts[null_counts > 0]
    if not bad_nulls.empty:
        sample = df[df[required_not_null].isna().any(axis=1)].head(20)
        log_event(
            "error",
            "ranking_rows_missing_required_values",
            null_counts=bad_nulls.to_dict(),
            sample=sample.to_dict(orient="records"),
        )
        raise ValueError(
            f"Ranking rows have nulls in required fields: {bad_nulls.to_dict()}"
        )

    invalid_usage = df[~df["ranking_usage"].isin(["edge", "context_only"])]
    if not invalid_usage.empty:
        raise ValueError("Ranking output contains invalid ranking_usage values.")

    excluded_rows = df[df["data_quality_status"] == "exclude"]
    if not excluded_rows.empty:
        raise ValueError("Ranking output contains data_quality_status='exclude' rows.")

    context_edge_rows = df[
        (df["ranking_usage"] == "context_only")
        & (
            (df["edge_language_allowed"] == True)
            | (df["include_in_core_area_advantage"] == True)
            | (df["confidence_eligible"] == True)
        )
    ]
    if not context_edge_rows.empty:
        raise ValueError("Context-only ranking rows contain edge/core/confidence flags.")

    bad_lens_tags = df[
        ~df["lens_tags"].apply(lambda value: isinstance(value, list))
    ]
    if not bad_lens_tags.empty:
        log_event(
            "error",
            "ranking_rows_invalid_lens_tags",
            rows=len(bad_lens_tags),
            sample=bad_lens_tags[["metric", "lens_tags"]]
            .head(20)
            .to_dict(orient="records"),
        )
        raise ValueError("Ranking rows have invalid lens_tags values; expected list.")

    as_of_ts = pd.to_datetime(df["as_of_date"])
    source_ts = pd.to_datetime(df["source_data_date"])

    if (source_ts > as_of_ts).any():
        bad = df[source_ts > as_of_ts].head(20)
        log_event(
            "error",
            "ranking_source_date_after_as_of_date",
            sample=bad[
                [
                    "season",
                    "as_of_date",
                    "source_data_date",
                    "window_type",
                    "metric",
                    "team_abv",
                ]
            ].to_dict(orient="records"),
        )
        raise ValueError("Ranking rows contain source_data_date after as_of_date.")

    if (df["data_lag_days"] < 0).any():
        raise ValueError("Ranking rows contain negative data_lag_days.")

    grain = ["season", "as_of_date", "window_type", "metric", "team_id"]
    dupes = df.duplicated(subset=grain, keep=False)
    if dupes.any():
        sample = df.loc[dupes, grain + ["team_abv", "value", "league_rank"]].head(20)
        log_event(
            "error",
            "ranking_grain_duplicates_seen",
            rows=int(dupes.sum()),
            sample=sample.to_dict(orient="records"),
        )
        raise ValueError("Duplicate ranking grain rows found.")

    bad_rank_range = df[
        (df["league_rank"] < 1) | (df["league_rank"] > df["teams_ranked"])
    ]
    if not bad_rank_range.empty:
        raise ValueError("league_rank values outside expected range.")

    bad_percentile = df[
        (df["league_percentile"] < 0) | (df["league_percentile"] > 100)
    ]
    if not bad_percentile.empty:
        raise ValueError("league_percentile values outside 0-100 range.")

    # Directional sanity check. For each group, rank 1 should match the expected
    # extreme value based on rank_direction.
    check_cols = ["season", "as_of_date", "window_type", "metric"]
    bad_direction_groups: List[Dict[str, Any]] = []

    for group_key, group in df.groupby(check_cols):
        rank_direction = str(group["rank_direction"].iloc[0])
        rank_1 = group[group["league_rank"] == 1]

        if rank_1.empty:
            bad_direction_groups.append({"group": group_key, "reason": "no_rank_1"})
            continue

        if rank_direction == "lower_is_better":
            expected = group["value"].min()
        else:
            expected = group["value"].max()

        if not (rank_1["value"] == expected).all():
            bad_direction_groups.append(
                {
                    "group": group_key,
                    "reason": "rank_1_value_not_expected_extreme",
                    "rank_direction": rank_direction,
                    "expected": expected,
                    "rank_1_values": rank_1["value"].tolist(),
                }
            )

        if len(bad_direction_groups) >= 10:
            break

    if bad_direction_groups:
        log_event(
            "error",
            "ranking_direction_validation_failed",
            sample=bad_direction_groups,
        )
        raise ValueError("Ranking direction validation failed.")

    # Product warning only: late-season regular-season groups should usually be
    # near/full 32 teams after carry-forward. We log visibility without failing,
    # because early dates and missing/null metric values can still legitimately
    # produce smaller groups.
    team_count_summary = (
        df.groupby(["window_type", "ranking_kind"])
        .agg(
            min_teams_ranked=("teams_ranked", "min"),
            max_teams_ranked=("teams_ranked", "max"),
        )
        .reset_index()
        .to_dict(orient="records")
    )

    summary_df = (
        df.groupby(["window_type", "ranking_kind"])
        .agg(
            rows=("metric", "count"),
            metrics=("metric", "nunique"),
            teams=("team_id", "nunique"),
            first_as_of_date=("as_of_date", "min"),
            last_as_of_date=("as_of_date", "max"),
            max_data_lag_days=("data_lag_days", "max"),
        )
        .reset_index()
    )
    summary_df["first_as_of_date"] = summary_df["first_as_of_date"].astype(str)
    summary_df["last_as_of_date"] = summary_df["last_as_of_date"].astype(str)

    log_event(
        "info",
        "rankings_df_validation_passed",
        season=season,
        rows=len(df),
        team_count_summary=team_count_summary,
        summary=summary_df.to_dict(orient="records"),
    )


# -----------------------------------------------------------------------------
# Write
# -----------------------------------------------------------------------------

def _resolve_write_disposition(if_exists: str) -> str:
    if if_exists == "replace":
        return bigquery.WriteDisposition.WRITE_TRUNCATE
    if if_exists == "append":
        return bigquery.WriteDisposition.WRITE_APPEND
    if if_exists == "fail":
        return bigquery.WriteDisposition.WRITE_EMPTY
    raise ValueError(f"Unsupported if_exists value: {if_exists}")


def write_rankings_table(
    df: pd.DataFrame,
    season: str,
    if_exists: str = "replace",
    recreate_table: bool = False,
) -> str:
    """Write ranking rows to BigQuery."""
    client = bigquery.Client(project=PROJECT)
    table_id = ensure_ranking_table(
        client,
        season=season,
        recreate_table=recreate_table,
    )

    destination_table = OUTPUT_TABLE_TEMPLATE.format(season=season)
    write_disposition = _resolve_write_disposition(if_exists)

    log_event(
        "info",
        "writing_team_metric_rankings",
        table=destination_table,
        rows=len(df),
        if_exists=if_exists,
        recreate_table=recreate_table,
        write_disposition=str(write_disposition),
    )

    table = client.get_table(table_id)

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
        "team_metric_rankings_written",
        table=destination_table,
        rows=len(df),
    )

    return table_id


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

def run_build_team_metric_rankings(
    season: str = "2025",
    if_exists: str = "replace",
    write: bool = True,
    recreate_table: bool = False,
) -> pd.DataFrame:
    """Build and optionally write Analytics.team_metric_rankings_{season}."""
    setup_logging()
    log_event(
        "info",
        "team_metric_rankings_job_started",
        season=season,
        write=write,
        recreate_table=recreate_table,
    )

    client = bigquery.Client(project=PROJECT)
    rankings_df = build_rankings_dataframe(client, season)

    if write:
        table_id = write_rankings_table(
            rankings_df,
            season=season,
            if_exists=if_exists,
            recreate_table=recreate_table,
        )
        log_event(
            "info",
            "team_metric_rankings_job_complete",
            season=season,
            table=table_id,
            rows=len(rankings_df),
        )
    else:
        log_event(
            "info",
            "team_metric_rankings_dry_run_complete",
            season=season,
            rows=len(rankings_df),
        )

    return rankings_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build GameLens league-wide as-of team metric rankings."
    )
    parser.add_argument(
        "--season",
        default="2025",
        help="NFL season to build, for example: 2025.",
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
    parser.add_argument(
        "--recreate-table",
        action="store_true",
        help="Delete and recreate the destination ranking table before writing.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_build_team_metric_rankings(
        season=str(args.season),
        if_exists=args.if_exists,
        write=not args.dry_run,
        recreate_table=bool(args.recreate_table),
    )
