import pandas as pd
from google.cloud import bigquery
from typing import List
from datetime import datetime, timedelta

from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
)
from utils.logging_setup import log_event
from utils.response_helpers import save_raw_response
from runtime_config import load_runtime_config

RUNTIME_CONFIG = load_runtime_config()
PROJECT = RUNTIME_CONFIG.project_id
TABLE_SCHEDULE = RUNTIME_CONFIG.league_table("schedule")
bq = bigquery.Client(project=PROJECT)

# ────────────────────────────────────────────────────────────────
# Step 0 – Insert helper (standard BigQuery insert)
# ────────────────────────────────────────────────────────────────
def insert_rows_nfl_games(client: bigquery.Client, rows: List[dict]):
    errors = client.insert_rows_json(TABLE_SCHEDULE, rows)
    if errors:
        for err in errors:
            log_event("error", "bq_insert_error_detail", details=err)
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    log_event("info", "bq_insert_success", rows=len(rows))

# ────────────────────────────────────────────────────────────────
# Step 1 – Yesterday: Only delete & re-insert if API returns data
# ────────────────────────────────────────────────────────────────
def process_yesterday_games(table_id, api_url, headers, yesterday_date):
    """
    Step 1: Handle yesterday's games:
    - Fetch from API.
    - Only delete and re-insert if games exist.
    """
    try:
        # Step 1a: Fetch yesterday's games from API
        raw_games = fetch_games_for_date(api_url, headers, yesterday_date)
        log_event("info", "yesterday_games_fetched", game_date=yesterday_date, count=len(raw_games))
        save_raw_response({"body": raw_games}, yesterday_date, prefix="nfl_games")

        # Step 1b: Only proceed if games exist
        if not raw_games:
            log_event("warning", "no_games_found_yesterday", game_date=yesterday_date)
            return {
                "status": "no_op",
                "game_date": yesterday_date,
                "selected_game_ids": [],
                "inserted_row_count": 0,
                "no_op_reason": "no_games_returned",
            }

        # Step 1c: Transform records
        df = transform_game_records(raw_games)
        if df.empty:
            log_event("warning", "no_games_after_transform_yesterday", game_date=yesterday_date)
            return {
                "status": "no_op",
                "game_date": yesterday_date,
                "selected_game_ids": [],
                "inserted_row_count": 0,
                "no_op_reason": "no_games_after_transform",
            }

        # Step 1d: Delete and insert all rows for yesterday's gameIDs
        game_ids = df["gameID"].tolist()
        if game_ids:
            delete_query = f"""
                DELETE FROM `{table_id}` WHERE gameID IN UNNEST(@game_ids)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("game_ids", "STRING", game_ids)]
            )
            bq.query(delete_query, job_config=job_config).result()
            log_event("info", "yesterday_games_deleted", count=len(game_ids))

        # Step 1e: Insert fresh records
        insert_into_bigquery(table_id, df.to_dict(orient="records"))
        log_event("info", "yesterday_games_inserted", game_date=yesterday_date, inserted=len(df))
        return {
            "status": "success",
            "game_date": yesterday_date,
            "selected_game_ids": [str(game_id) for game_id in game_ids],
            "inserted_row_count": len(df),
        }

    except Exception as e:
        log_event("error", "yesterday_games_process_failed", game_date=yesterday_date, error=str(e))
        raise

# ────────────────────────────────────────────────────────────────
# Step 2 – Today and next 2 days: Delete and insert by gameID
# ────────────────────────────────────────────────────────────────
def process_game_date(table_id, api_url, headers, game_date):
    """
    Step 2: For each date (today and next 2 days)
    - Fetch from API.
    - Delete all rows for matching gameIDs.
    - Insert all fresh data.
    """
    try:
        # Step 2a: Fetch games from API
        raw_games = fetch_games_for_date(api_url, headers, game_date)
        log_event("info", "games_fetched", game_date=game_date, count=len(raw_games))
        save_raw_response({"body": raw_games}, game_date, prefix="nfl_games")

        # Step 2b: Transform API records into DataFrame
        df = transform_game_records(raw_games)
        if df.empty:
            log_event("warning", "no_games_after_transform", game_date=game_date)
            return {
                "status": "no_op",
                "game_date": game_date,
                "selected_game_ids": [],
                "inserted_row_count": 0,
                "no_op_reason": "no_games_after_transform",
            }

        # Step 2c: Delete all rows in BigQuery for these gameIDs
        game_ids = df["gameID"].tolist()
        if game_ids:
            delete_query = f"""
                DELETE FROM `{table_id}` WHERE gameID IN UNNEST(@game_ids)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("game_ids", "STRING", game_ids)]
            )
            bq.query(delete_query, job_config=job_config).result()
            log_event("info", "existing_games_deleted", count=len(game_ids))

        # Step 2d: Insert all fresh records
        insert_into_bigquery(table_id, df.to_dict(orient="records"))
        log_event("info", "games_inserted", game_date=game_date, inserted=len(df))
        return {
            "status": "success",
            "game_date": game_date,
            "selected_game_ids": [str(game_id) for game_id in game_ids],
            "inserted_row_count": len(df),
        }

    except Exception as e:
        log_event("error", "games_fetch_or_insert_failed", game_date=game_date, error=str(e))
        raise

# ────────────────────────────────────────────────────────────────
# Step 3 – Fetch games for a single date from API
# ────────────────────────────────────────────────────────────────
def fetch_games_for_date(api_url, headers, game_date):
    """
    Step 3: Make API call to fetch games for a given date.
    """
    query = {"gameDate": game_date}
    games = fetch_and_validate_api_data(api_url, headers, query)
    if not games or "body" not in games:
        raise ValueError(f"No valid response for date {game_date}")
    return games["body"]

# ────────────────────────────────────────────────────────────────
# Step 4 – Transform API records into BigQuery-ready DataFrame
# ────────────────────────────────────────────────────────────────
def transform_game_records(game_body):
    """
    Step 4: Transform raw API records (list of dicts) into a DataFrame ready for BigQuery insertion.
    Handles types, serialization, missing columns, and BigQuery compatibility.
    """
    if not game_body:
        return pd.DataFrame()  # Return empty DataFrame

    df = pd.DataFrame(game_body)

    # Ensure all required columns exist, fill missing if necessary
    required_cols = [
        "gameID", "seasonType", "away", "gameDate", "espnID", "teamIDHome",
        "gameStatus", "gameWeek", "teamIDAway", "home", "espnLink", "cbsLink",
        "gameTime", "gameTime_epoch", "season", "neutralSite", "gameStatusCode",
        "boxscore_loaded", "score_loaded"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Type handling and serialization
    # 1. Date (gameDate)
    df["gameDate"] = pd.to_datetime(df["gameDate"], format="%Y%m%d").dt.date

    # 2. Timestamp (gameTime_epoch) as string for BigQuery (keep time even at midnight; coerce bad to NULL)
    epoch_num = pd.to_numeric(df["gameTime_epoch"], errors="coerce")
    dt_utc = pd.to_datetime(epoch_num, unit="s", utc=True)
    df["gameTime_epoch"] = dt_utc.dt.strftime("%Y-%m-%d %H:%M:%S")
    df.loc[dt_utc.isna(), "gameTime_epoch"] = None  # NaT -> NULL for BQ

    # 3. Neutral site as boolean
    df["neutralSite"] = df["neutralSite"].astype(str).str.lower() == "true"
    # 4. Add loaded flags
    df["boxscore_loaded"] = False
    df["score_loaded"] = False
    # 5. Ensure proper column order
    df = df[required_cols]
    # 6. BigQuery wants date as 'YYYY-MM-DD' string
    df["gameDate"] = df["gameDate"].apply(lambda x: x.strftime("%Y-%m-%d") if not pd.isnull(x) else None)

    return df  # Return DataFrame, not dict

# ────────────────────────────────────────────────────────────────
# Step 5 – Master controller: Fetch yesterday, today, and next 2 days
# ────────────────────────────────────────────────────────────────
def fetch_nfl_games(load_date=None):
    """
    Step 5: Main ETL routine.
    - Step 5a: If not a historical run, first process yesterday (safe delete & insert).
    - Step 5b: Then, process today and next 2 days (delete & insert for each date).
    """
    if RUNTIME_CONFIG.is_controlled_replay:
        log_event(
            "info",
            "schedule_replay_skipped",
            load_date=load_date,
        )
        return {
            "status": "no_op",
            "requested_dates": [],
            "successful_dates": [],
            "no_op_dates": [],
            "selected_game_count": 0,
            "selected_game_ids": [],
            "successful_game_count": 0,
            "inserted_row_count": 0,
            "failed_date_count": 0,
            "failures": [],
            "no_op_reason": "controlled_replay_schedule_skipped",
        }

    is_historical_run = (
        load_date is not None and
        datetime.strptime(load_date, "%Y-%m-%d").date() != datetime.now().date()
    )
    start_date = datetime.strptime(load_date, "%Y-%m-%d") if load_date else datetime.now()

    if is_historical_run:
        log_event("info", "historical_schedule_skipped", load_date=load_date)
        return {
            "status": "no_op",
            "requested_dates": [],
            "successful_dates": [],
            "no_op_dates": [],
            "selected_game_count": 0,
            "selected_game_ids": [],
            "successful_game_count": 0,
            "inserted_row_count": 0,
            "failed_date_count": 0,
            "failures": [],
            "no_op_reason": "historical_schedule_skipped",
        }

    table_id = TABLE_SCHEDULE
    api_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForDate"
    headers = {
        "x-rapidapi-key": get_secret("Tank_Rapidapi"),
        "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }
    yesterday_date = (start_date + timedelta(days=-1)).strftime("%Y%m%d")
    dated_steps = [
        (process_yesterday_games, yesterday_date),
        *[
            (
                process_game_date,
                (start_date + timedelta(days=offset)).strftime("%Y%m%d"),
            )
            for offset in range(3)
        ],
    ]
    requested_dates = [game_date for _, game_date in dated_steps]
    successful_dates = []
    no_op_dates = []
    selected_game_ids = []
    inserted_row_count = 0
    failures = []

    for processor, game_date in dated_steps:
        try:
            result = processor(table_id, api_url, headers, game_date)
            if result["status"] == "success":
                successful_dates.append(game_date)
                selected_game_ids.extend(result["selected_game_ids"])
                inserted_row_count += result["inserted_row_count"]
            else:
                no_op_dates.append(game_date)
        except Exception as exc:
            failures.append({
                "game_date": game_date,
                "error": str(exc),
            })
            log_event(
                "error",
                "schedule_date_failed",
                game_date=game_date,
                error=str(exc),
            )

    completed_date_count = len(successful_dates) + len(no_op_dates)
    if failures and completed_date_count:
        status = "partial_failure"
    elif failures:
        status = "failed"
    elif successful_dates:
        status = "success"
    else:
        status = "no_op"

    summary = {
        "status": status,
        "requested_dates": requested_dates,
        "successful_dates": successful_dates,
        "no_op_dates": no_op_dates,
        "selected_game_count": len(selected_game_ids),
        "selected_game_ids": selected_game_ids,
        "successful_game_count": len(selected_game_ids),
        "inserted_row_count": inserted_row_count,
        "failed_date_count": len(failures),
        "failures": failures,
    }
    if status == "no_op":
        summary["no_op_reason"] = "no_schedule_rows_inserted"

    log_event(
        "info",
        "nfl_games_job_completed",
        status=status,
        inserted=inserted_row_count,
        failed_dates=len(failures),
    )
    return summary
