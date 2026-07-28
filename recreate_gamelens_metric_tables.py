"""
Recreate GameLens metric fact/windowed BigQuery tables with explicit schemas.

Purpose
-------
This utility deletes and recreates the season-specific GameLens metric tables:

- Analytics.game_team_metric_facts_{season}
- Analytics.team_metrics_windowed_{season}

It is intended for schema reset work after metric_registry.py metadata changes.

Key behavior
------------
- Dry-run by default. Nothing is deleted unless --execute is passed.
- Deletes each selected table if it exists.
- Recreates each selected table with full column descriptions.
- Stores lens_tags as REPEATED STRING for a cleaner warehouse/frontend-friendly shape.

Example dry run
---------------
python recreate_gamelens_metric_tables.py --seasons 2023 2024 2025

Execute
-------
python recreate_gamelens_metric_tables.py --seasons 2023 2024 2025 --execute

Only recreate facts
-------------------
python recreate_gamelens_metric_tables.py --seasons 2025 --table-scope facts --execute

Only recreate windowed
----------------------
python recreate_gamelens_metric_tables.py --seasons 2025 --table-scope windowed --execute

Important
---------
This script only creates empty tables with the desired schema.
After running with --execute, rerun the builders:

python build_metric_facts.py --season 2025
python build_windowed_metrics.py --season 2025

Repeat for each season as needed.
"""

from __future__ import annotations

import argparse
from typing import Iterable, List

from google.cloud import bigquery


DEFAULT_PROJECT_ID = "nfl-stream-406420"
DEFAULT_DATASET_ID = "Analytics"
DEFAULT_SEASONS = ["2023", "2024", "2025"]


# -----------------------------------------------------------------------------
# Shared metric metadata schema
# -----------------------------------------------------------------------------

def metric_metadata_schema() -> List[bigquery.SchemaField]:
    """Metadata fields attached from analytics.metric_registry.py."""
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
            description="Metric category used for product grouping, such as Passing Game, Red Zone Finish, or Pressure.",
        ),
        bigquery.SchemaField(
            "core_area",
            "STRING",
            mode="REQUIRED",
            description="High-level matchup area the metric belongs to, such as Offensive Output, Scoring Efficiency, Defensive Control, Disruption and Turnovers, or Field Control (Special Teams).",
        ),
        bigquery.SchemaField(
            "comparison_direction",
            "STRING",
            mode="REQUIRED",
            description="How values compare: higher, lower, or context. Higher/lower can support directional ranking; context is descriptive only.",
        ),
        bigquery.SchemaField(
            "higher_is_better",
            "BOOLEAN",
            mode="NULLABLE",
            description="Boolean comparison helper. True for higher-is-better metrics, False for lower-is-better metrics, NULL for context metrics.",
        ),
        bigquery.SchemaField(
            "raw_or_derived",
            "STRING",
            mode="REQUIRED",
            description="Whether the metric is raw, derived, or contextual according to metric_registry.py.",
        ),
        bigquery.SchemaField(
            "aggregation_method",
            "STRING",
            mode="REQUIRED",
            description="How the metric should aggregate across games, such as sum, ratio_from_sums, mean_contextual, or latest.",
        ),
        bigquery.SchemaField(
            "numerator",
            "STRING",
            mode="NULLABLE",
            description="Numerator metric used for ratio_from_sums derived metrics. NULL for non-ratio metrics.",
        ),
        bigquery.SchemaField(
            "denominator",
            "STRING",
            mode="NULLABLE",
            description="Denominator metric used for ratio_from_sums derived metrics. NULL for non-ratio metrics.",
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
            description="Frontend/product grouping tags from metric_registry.py. Stored as repeated strings for easy filtering and UNNEST queries.",
        ),
    ]


# -----------------------------------------------------------------------------
# Table schemas
# -----------------------------------------------------------------------------

def game_team_metric_facts_schema() -> List[bigquery.SchemaField]:
    """Schema for Analytics.game_team_metric_facts_{season}."""
    return [
        bigquery.SchemaField(
            "season",
            "STRING",
            mode="REQUIRED",
            description="NFL season year, such as 2025.",
        ),
        bigquery.SchemaField(
            "game_id",
            "STRING",
            mode="REQUIRED",
            description="Unique game identifier in format YYYYMMDD_AWAY@HOME.",
        ),
        bigquery.SchemaField(
            "game_date",
            "DATE",
            mode="REQUIRED",
            description="Game date in local/league schedule date.",
        ),
        bigquery.SchemaField(
            "game_datetime_raw",
            "STRING",
            mode="NULLABLE",
            description="Raw game datetime value from League.schedule, preserved for audit/debugging.",
        ),
        bigquery.SchemaField(
            "game_week",
            "STRING",
            mode="NULLABLE",
            description="Original schedule week label, such as Preseason Week 1, Week 14, Wild Card, Divisional Round, Conference Championship, or Super Bowl.",
        ),
        bigquery.SchemaField(
            "season_type",
            "STRING",
            mode="NULLABLE",
            description="Original season type from League.schedule, such as Regular Season or Postseason.",
        ),
        bigquery.SchemaField(
            "season_phase",
            "STRING",
            mode="REQUIRED",
            description="Normalized season phase: preseason, regular_season, postseason, or unknown.",
        ),
        bigquery.SchemaField(
            "phase_week",
            "INTEGER",
            mode="NULLABLE",
            description="Normalized week number within the season phase. For postseason, uses playoff round order.",
        ),
        bigquery.SchemaField(
            "global_week_order",
            "INTEGER",
            mode="NULLABLE",
            description="Sortable global week order used for phase-aware windows. Preseason starts low, regular season starts at 100, postseason starts at 201.",
        ),
        bigquery.SchemaField(
            "is_preseason",
            "BOOLEAN",
            mode="REQUIRED",
            description="True when the game is classified as preseason.",
        ),
        bigquery.SchemaField(
            "is_regular_season",
            "BOOLEAN",
            mode="REQUIRED",
            description="True when the game is classified as regular season.",
        ),
        bigquery.SchemaField(
            "is_postseason",
            "BOOLEAN",
            mode="REQUIRED",
            description="True when the game is classified as postseason.",
        ),
        bigquery.SchemaField(
            "is_playoff_game",
            "BOOLEAN",
            mode="REQUIRED",
            description="True when the game is a playoff/postseason game.",
        ),
        bigquery.SchemaField(
            "team_id",
            "STRING",
            mode="REQUIRED",
            description="Team identifier from the source schedule/metrics data.",
        ),
        bigquery.SchemaField(
            "team_abv",
            "STRING",
            mode="REQUIRED",
            description="Team abbreviation, such as BUF, PHI, or DET.",
        ),
        bigquery.SchemaField(
            "team_type",
            "STRING",
            mode="NULLABLE",
            description="Team side in the game: away or home.",
        ),
        bigquery.SchemaField(
            "metric",
            "STRING",
            mode="REQUIRED",
            description="Canonical metric key from metric_registry.py.",
        ),
        bigquery.SchemaField(
            "value",
            "FLOAT",
            mode="REQUIRED",
            description="Game-level metric value for this team/game/metric row.",
        ),
        *metric_metadata_schema(),
        bigquery.SchemaField(
            "created_at",
            "TIMESTAMP",
            mode="REQUIRED",
            description="Timestamp when this cleaned fact row was generated.",
        ),
    ]


def team_metrics_windowed_schema() -> List[bigquery.SchemaField]:
    """Schema for Analytics.team_metrics_windowed_{season}."""
    return [
        bigquery.SchemaField(
            "season",
            "STRING",
            mode="REQUIRED",
            description="NFL season year, such as 2025.",
        ),
        bigquery.SchemaField(
            "team_id",
            "STRING",
            mode="REQUIRED",
            description="Team identifier from the source schedule/metrics data.",
        ),
        bigquery.SchemaField(
            "team_abv",
            "STRING",
            mode="REQUIRED",
            description="Team abbreviation, such as BUF, PHI, or DET.",
        ),
        bigquery.SchemaField(
            "data_date",
            "DATE",
            mode="REQUIRED",
            description="As-of data date for the windowed metric snapshot. Represents the latest included game date in the window.",
        ),
        bigquery.SchemaField(
            "window_type",
            "STRING",
            mode="REQUIRED",
            description="Window definition used for the metric, such as regular_season_to_date, regular_plus_postseason_to_date, last_3_games, last_7_games, or preseason_to_date.",
        ),
        bigquery.SchemaField(
            "games_in_window",
            "INTEGER",
            mode="REQUIRED",
            description="Number of games included in this team/window snapshot.",
        ),
        bigquery.SchemaField(
            "window_start_date",
            "DATE",
            mode="REQUIRED",
            description="Earliest game date included in this window.",
        ),
        bigquery.SchemaField(
            "window_end_date",
            "DATE",
            mode="REQUIRED",
            description="Latest game date included in this window.",
        ),
        bigquery.SchemaField(
            "latest_included_game_id",
            "STRING",
            mode="REQUIRED",
            description="Most recent game_id included in the window.",
        ),
        bigquery.SchemaField(
            "latest_included_global_week_order",
            "INTEGER",
            mode="NULLABLE",
            description="Global week order for the latest included game in the window.",
        ),
        bigquery.SchemaField(
            "metric",
            "STRING",
            mode="REQUIRED",
            description="Canonical metric key from metric_registry.py.",
        ),
        bigquery.SchemaField(
            "value",
            "FLOAT",
            mode="NULLABLE",
            description="Windowed metric value. Derived rate metrics are recalculated from summed ingredients where configured; value may be NULL when denominator/input data is unavailable.",
        ),
        *metric_metadata_schema(),
        bigquery.SchemaField(
            "created_at",
            "TIMESTAMP",
            mode="REQUIRED",
            description="Timestamp when this windowed metric row was generated.",
        ),
    ]


# -----------------------------------------------------------------------------
# Table creation helpers
# -----------------------------------------------------------------------------

def full_table_id(project_id: str, dataset_id: str, table_name: str) -> str:
    return f"{project_id}.{dataset_id}.{table_name}"


def recreate_table(
    client: bigquery.Client,
    *,
    project_id: str,
    dataset_id: str,
    table_name: str,
    schema: List[bigquery.SchemaField],
    description: str,
    execute: bool,
) -> None:
    table_id = full_table_id(project_id, dataset_id, table_name)

    print(f"\n--- {table_id} ---")
    print(f"Columns: {len(schema)}")
    print(f"Action: {'DELETE + CREATE' if execute else 'DRY RUN ONLY'}")

    if not execute:
        return

    client.delete_table(table_id, not_found_ok=True)
    print(f"Deleted if existed: {table_id}")

    table = bigquery.Table(table_id, schema=schema)
    table.description = description
    table.labels = {
        "app": "gamelens",
        "domain": "nfl",
        "managed_by": "python",
    }

    created_table = client.create_table(table)
    print(f"Created: {created_table.full_table_id}")


def recreate_metric_tables(
    *,
    project_id: str,
    dataset_id: str,
    seasons: Iterable[str],
    table_scope: str,
    execute: bool,
) -> None:
    client = bigquery.Client(project=project_id)

    for season in seasons:
        season = str(season)

        if table_scope in {"all", "facts"}:
            recreate_table(
                client,
                project_id=project_id,
                dataset_id=dataset_id,
                table_name=f"game_team_metric_facts_{season}",
                schema=game_team_metric_facts_schema(),
                description=(
                    "Cleaned GameLens game/team/metric fact table. "
                    "Built from Analytics.game_metrics_flat joined to League.schedule, "
                    "with authoritative metadata attached from analytics.metric_registry.py."
                ),
                execute=execute,
            )

        if table_scope in {"all", "windowed"}:
            recreate_table(
                client,
                project_id=project_id,
                dataset_id=dataset_id,
                table_name=f"team_metrics_windowed_{season}",
                schema=team_metrics_windowed_schema(),
                description=(
                    "GameLens phase-aware and rolling team metric windows. "
                    "Built from Analytics.game_team_metric_facts_{season}; derived rates are recalculated from summed ingredients."
                ),
                execute=execute,
            )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete and recreate GameLens metric BigQuery tables with explicit schemas."
    )
    parser.add_argument(
        "--project-id",
        default=DEFAULT_PROJECT_ID,
        help=f"BigQuery project ID. Default: {DEFAULT_PROJECT_ID}",
    )
    parser.add_argument(
        "--dataset-id",
        default=DEFAULT_DATASET_ID,
        help=f"BigQuery dataset ID. Default: {DEFAULT_DATASET_ID}",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=DEFAULT_SEASONS,
        help="Season(s) to recreate. Example: --seasons 2023 2024 2025",
    )
    parser.add_argument(
        "--table-scope",
        choices=["all", "facts", "windowed"],
        default="all",
        help="Which table group to recreate. Default: all.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete and recreate tables. Without this flag, the script only prints a dry run.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if not args.execute:
        print("DRY RUN: no tables will be deleted or created.")
        print("Add --execute to perform the destructive table reset.")

    recreate_metric_tables(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        seasons=args.seasons,
        table_scope=args.table_scope,
        execute=args.execute,
    )
