import pandas as pd
import builtins

_old_to_dict = pd.DataFrame.to_dict

def patched_to_dict(self, *args, **kwargs):
    if kwargs.get("orient") == "index":
        print("DEBUG: to_dict(orient='index') called here!")
        import traceback; traceback.print_stack()
    return _old_to_dict(self, *args, **kwargs)

pd.DataFrame.to_dict = patched_to_dict


from google.cloud import bigquery
import pandas as pd
from utils.logging_setup import log_event, setup_logging
from datetime import datetime
import json
from pandas_gbq import to_gbq
from typing import Union

PROJECT = "nfl-stream-406420"
SCHEDULE_TABLE = "League.schedule"
METRICS_TABLE = "Analytics.game_metrics_flat"

def log_to_file(filename: str, label: str, content: Union[dict, list]) -> None:  # ✅ change here
    with open(filename, "a", encoding="utf-8") as f:
        f.write(f"\n--- {label} @ {datetime.now().isoformat()} ---\n")
        f.write(json.dumps(content, indent=2, default=str))
        f.write("\n")
        
# ---------------------------------------------
# Step 1: Get date range for given season
# ---------------------------------------------
def get_season_date_bounds(client: bigquery.Client, season: str) -> tuple:
    query = f"""
        SELECT MIN(gameDate) AS min_date, MAX(gameDate) AS max_date
        FROM `{PROJECT}.{SCHEDULE_TABLE}`
        WHERE season = @season
        AND (gameStatus = 'Final' OR gameStatus = 'Final/OT')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("season", "STRING", season)
        ]
    )
    result = client.query(query, job_config=job_config).to_dataframe().iloc[0]
    return result["min_date"], result["max_date"]

# ---------------------------------------------
# Step 2: Load all game metrics within date range
# ---------------------------------------------
def load_game_metrics(client: bigquery.Client, start_date: str, end_date: str, limit: int = None) -> pd.DataFrame:
    base_query = f"""
        SELECT *
        FROM `{PROJECT}.{METRICS_TABLE}`
        WHERE data_date BETWEEN @start_date AND @end_date
        ORDER BY data_date ASC
    """

    if limit:
        base_query += f"\nLIMIT {limit}"

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    return client.query(base_query, job_config=job_config).to_dataframe()

# ---------------------------------------------
# Step 3: Config for derived, raw, and classification metadata
# ---------------------------------------------
# This is the single source of truth for display grouping.
# core_area must remain one of:
#   - Disruption and Turnovers
#   - Field Control (Special Teams)
#   - Offensive Output
#   - Scoring Efficiency
#   - Defensive Control
METRIC_CLASSIFICATION = {
    "Offensive Output": {
        "Passing Game": [
            "passing_yards",
            "pass_completions",
            "pass_attempts",
            "passing_tds",
            "yards_per_pass",
        ],
        "Rushing Game": [
            "rushing_yards",
            "rushing_attempts",
            "rushing_tds",
            "yards_per_rush",
        ],
        "Offensive Rhythm": [
            "total_yards",
            "total_plays",
            "yards_per_play",
            "first_downs",
            "1st_down_rate",
            "total_drives",
            "time_of_possession",
            "run_play_pct",
            "pass_play_pct",
            "pass_run_ratio",
            "passing_tds_rushing_tds_sum",
            "pass_td_share",
            "rush_td_share",
            "total_offensive_snaps",
            "offensive_snap_load",
        ],
    },
    "Scoring Efficiency": {
        "Scoring Production": [
            "actual_points",
            "points_per_play",
            "td_rate",
        ],
        "Red Zone Finish": [
            "red_zone_tds",
            "red_zone_attempts",
            "red_zone_efficiency",
        ],
        "Drive Conversion": [
            "third_down_conversions",
            "third_down_attempts",
            "third_down_pct",
            "fourth_down_conversions",
            "fourth_down_attempts",
            "fourth_down_pct",
        ],
    },
    "Defensive Control": {
        "Yardage Suppression": [
            "yards_allowed",
            "opponent_total_plays",
            "defensive_success_rate",
            "points_allowed_per_yard",
        ],
        "Scoring Suppression": [
            "points_allowed",
            "points_allowed_per_play",
        ],
        "Defensive Workload": [
            "total_defensive_snaps",
            "defensive_snap_load",
        ],
    },
    "Disruption and Turnovers": {
        "Pressure": [
            "sacks",
            "pressure_rate",
            "sacks_taken",
            "sack_yards_lost",
            "sacks_plus_sacks_taken",
            "sack_to_turnover_ratio",
        ],
        "Turnovers": [
            "defensive_interceptions",
            "fumbles_recovered",
            "interceptions_thrown",
            "fumbles_lost",
            "turnover_margin",
        ],
    },
    "Field Control (Special Teams)": {
        "Special Teams Usage": [
            "total_special_teams_snaps",
            "special_teams_snap_pct",
            "total_snaps",
        ],
    },
}
def build_metric_meta(classification: dict) -> dict:
    """Flatten core_area -> category -> metrics into metric -> metadata."""
    meta = {}
    for core_area, categories in classification.items():
        for category, metrics in categories.items():
            for metric in metrics:
                if metric in meta:
                    raise ValueError(f"Metric classified more than once: {metric}")
                meta[metric] = {
                    "category": category,
                    "core_area": core_area,
                }
    return meta

METRIC_META = build_metric_meta(METRIC_CLASSIFICATION)

DERIVED_METRICS = {
    "points_per_play": {"numerator": "actual_points", "denominator": "total_plays"},
    "points_allowed_per_play": {"numerator": "points_allowed", "denominator": "opponent_total_plays"},
    "td_rate": {"numerator": "passing_tds_rushing_tds_sum", "denominator": "total_plays"},
    "red_zone_efficiency": {"numerator": "red_zone_tds", "denominator": "red_zone_attempts"},
    "third_down_pct": {"numerator": "third_down_conversions", "denominator": "third_down_attempts"},
    "fourth_down_pct": {"numerator": "fourth_down_conversions", "denominator": "fourth_down_attempts"},
    "run_play_pct": {"numerator": "rushing_attempts", "denominator": "total_plays"},
    "pass_play_pct": {"numerator": "pass_attempts", "denominator": "total_plays"},
    "pass_run_ratio": {"numerator": "pass_attempts", "denominator": "rushing_attempts"},
    "1st_down_rate": {"numerator": "first_downs", "denominator": "total_plays"},
    "pass_td_share": {"numerator": "passing_tds", "denominator": "passing_tds_rushing_tds_sum"},
    "rush_td_share": {"numerator": "rushing_tds", "denominator": "passing_tds_rushing_tds_sum"},
    "offensive_snap_load": {"numerator": "total_offensive_snaps", "denominator": "total_snaps"},
    "defensive_snap_load": {"numerator": "total_defensive_snaps", "denominator": "total_snaps"},
    "special_teams_snap_pct": {"numerator": "total_special_teams_snaps", "denominator": "total_snaps"},
    "sack_to_turnover_ratio": {"numerator": "sacks", "denominator": "defensive_interceptions"},
    "pressure_rate": {"numerator": "sacks_plus_sacks_taken", "denominator": "pass_attempts"},
    "turnover_margin": {"numerator": "turnover_margin", "denominator": None},
    "defensive_success_rate": {"numerator": "yards_allowed", "denominator": "opponent_total_plays", "inverse": True},
    "points_allowed_per_yard": {"numerator": "points_allowed", "denominator": "yards_allowed"},
    "yards_per_play": {"numerator": "total_yards", "denominator": "total_plays"},
    "yards_per_pass": {"numerator": "passing_yards", "denominator": "pass_attempts"},
    "yards_per_rush": {"numerator": "rushing_yards", "denominator": "rushing_attempts"},
}

RAW_METRICS = list(set([
    "actual_points", "points_allowed", "passing_yards", "rushing_yards", "total_yards",
    "sacks", "defensive_interceptions", "first_downs", "fumbles_recovered", "interceptions_thrown", "fumbles_lost",
    "time_of_possession", "third_down_conversions", "third_down_attempts", "fourth_down_conversions", "fourth_down_attempts",
    "total_plays", "total_drives", "red_zone_tds", "red_zone_attempts", "yards_allowed", "opponent_total_plays",
    "rushing_attempts", "pass_completions", "passing_tds", "rushing_tds", "total_offensive_snaps", "total_defensive_snaps",
    "total_special_teams_snaps", "total_snaps", "sacks_taken", "sack_yards_lost", "pass_attempts",
    "sacks_plus_sacks_taken", "passing_tds_rushing_tds_sum"
]))

def apply_metric_metadata() -> None:
    """Attach standardized category/core_area metadata to derived metrics."""
    configured_metrics = set(DERIVED_METRICS.keys()) | set(RAW_METRICS)
    classified_metrics = set(METRIC_META.keys())

    missing = sorted(configured_metrics - classified_metrics)
    extra = sorted(classified_metrics - configured_metrics)

    if missing or extra:
        raise ValueError(
            "Metric classification mismatch. "
            f"Missing from METRIC_CLASSIFICATION: {missing}. "
            f"Classified but not configured for output: {extra}."
        )

    for metric, meta in DERIVED_METRICS.items():
        meta.update(METRIC_META[metric])

apply_metric_metadata()

def get_metric_meta(metric: str) -> dict:
    """Return standardized metadata for a metric, failing loudly if unmapped."""
    if metric not in METRIC_META:
        raise KeyError(f"No category/core_area mapping found for metric: {metric}")
    return METRIC_META[metric]


# ---------------------------------------------
# Step 4: Aggregate incrementally by team/date
# ---------------------------------------------
def build_incremental_metrics(df: pd.DataFrame, season: str) -> pd.DataFrame:
    # Step 4A: Sort raw game_metrics_flat data by team and date
    df = df.sort_values(by=["team_id", "data_date"])

    # Step 4B: Pivot to wide format — one row per team/game, with columns for each metric
    pivot = df.pivot_table(
        index=["team_id", "team_abv", "data_date"],
        columns="metric",
        values="value",
        aggfunc="first"
    ).reset_index()

    log_event("info", f"pivot_table_created | season={season} | shape={pivot.shape} | sample_columns={list(pivot.columns[:10])}")

    # Step 4C: Ensure uniqueness before aggregation.
    # Category/core_area are now assigned from METRIC_CLASSIFICATION, not inherited from source rows.
    dupes_mask = df.duplicated(subset=["team_id", "data_date", "metric"], keep=False)
    if dupes_mask.any():
        log_event(
            "warning",
            "duplicate_metric_rows_seen",
            count=int(dupes_mask.sum()),
            sample=df.loc[dupes_mask, ["team_id","data_date","metric","category","core_area"]]
                   .sort_values(["team_id","data_date","metric"])
                   .head(20)
                   .to_dict(orient="records")
        )
        # Drop duplicates, keeping last (or first) consistently
        df = df.drop_duplicates(subset=["team_id", "data_date", "metric"], keep="last")

    results = []

    # Step 4D: Group data by team and calculate cumulative sums for each metric over time
    for team_id, team_df in pivot.groupby("team_id"):
        team_df = team_df.sort_values("data_date")
        
        # Calculate rolling/cumulative sum of all numeric metrics
        cumsum = team_df.drop(columns=["team_abv", "team_id", "data_date"]).cumsum()
        cumsum["team_id"] = team_id
        cumsum["team_abv"] = team_df["team_abv"].values[0]
        cumsum["data_date"] = team_df["data_date"].values
        cumsum["season"] = season

        log_event("info", f"cumsum_built | team_id={team_id} | num_rows={len(cumsum)} | dates={list(cumsum['data_date'].astype(str))} | metrics={list(cumsum.columns[:10])}")

        # Step 4E: Recalculate derived metrics based on summed inputs
        for metric, meta in DERIVED_METRICS.items():
            if meta["numerator"] == "passing_tds_rushing_tds_sum":
                numer = cumsum.get("passing_tds", 0) + cumsum.get("rushing_tds", 0)
            else:
                numer = cumsum.get(meta["numerator"], pd.Series([0] * len(cumsum)))

            if meta["denominator"]:
                denom = cumsum.get(meta["denominator"], pd.Series([1] * len(cumsum))).fillna(1).replace(0, 1)
                series = numer / denom
            else:
                series = numer

            if meta.get("inverse"):
                series = 1 - series

            for idx, row in cumsum.iterrows():
                val = series.loc[idx] if idx in series.index else None
                results.append({
                    "season": row["season"],
                    "data_date": row["data_date"],
                    "team_id": row["team_id"],
                    "team_abv": row["team_abv"],
                    "metric": metric,
                    "category": meta["category"],
                    "core_area": meta["core_area"],
                    "value": round(val, 3) if pd.notna(val) else None,
                })

        # Step 4F: Add raw metric values (like sacks, total yards, etc.) from the cumsum table
        for i, row in cumsum.iterrows():
            for metric in RAW_METRICS:
                if metric in row:
                    meta = get_metric_meta(metric)
                    results.append({
                        "season": row["season"],
                        "data_date": row["data_date"],
                        "team_id": row["team_id"],
                        "team_abv": row["team_abv"],
                        "metric": metric,
                        "category": meta["category"],
                        "core_area": meta["core_area"],
                        "value": round(row[metric], 3) if pd.notna(row[metric]) else None,
                    })

    # Step 4G: Return full long-format table
    result_df = pd.DataFrame(results)
    log_event("info", f"final_result_ready | season={season} | rows={len(result_df)}")

    log_to_file(
        filename="debug_metrics_sample.log",
        label=f"Season {season} — First 2 Rows",
        content=result_df.head(2).to_dict(orient="records")
    )
    
    return result_df

# ---------------------------------------------
# Entry point
# ---------------------------------------------
def run_aggregate_for_season(season: str = "2025", output_suffix: str = ""):
    setup_logging()
    client = bigquery.Client(project=PROJECT)

    log_event("info", f"season_start | season={season}")
    min_date, max_date = get_season_date_bounds(client, season)
    if not min_date or not max_date:
        log_event("warning", f"season_skipped | season={season}", reason="no final games")
        return

    df = load_game_metrics(client, min_date, max_date)
    log_event("info", f"metrics_loaded | season={season} | rows={len(df)}")

    result_df = build_incremental_metrics(df, season)
    log_event("info", f"metrics_aggregated | season={season} | rows={len(result_df)}")

    OUTPUT_TABLE = f"Analytics.team_metrics_season_{season}{output_suffix}"
    log_event("info", f"write_to_bq | table={OUTPUT_TABLE}")

    bad_rows = result_df[result_df[["season", "data_date", "team_id", "metric", "category", "core_area", "value"]].isnull().any(axis=1)]
    if not bad_rows.empty:
        print("❌ Bad rows with nulls:")
        print(bad_rows.head(10).to_string())
        bad_rows.to_csv("debug_bad_rows.csv", index=False)
        raise ValueError("Found rows with nulls in required fields.")

    to_gbq(result_df, destination_table=OUTPUT_TABLE, project_id=PROJECT, if_exists="replace")
    log_event("info", f"season_complete | season={season}")