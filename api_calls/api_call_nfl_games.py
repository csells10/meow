import pandas as pd
from google.cloud import bigquery
from typing import List
from datetime import datetime, timedelta

from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_records,
    filter_new_records,
    delete_yesterdays_games_from_bigquery
)
from utils.logging_setup import log_event
from utils.response_helpers import save_raw_response

PROJECT = "nfl-stream-406420"
TABLE_SCHEDULE = f"{PROJECT}.League.schedule_dev"
bq = bigquery.Client(project=PROJECT)

# ────────────────────────────────────────────────────────────────
# Step 0.5 – Setup BigQuery client
# ────────────────────────────────────────────────────────────────
def insert_rows_nfl_games(client: bigquery.Client, rows: List[dict]):
    errors = client.insert_rows_json(TABLE_SCHEDULE, rows)
    if errors:
        for err in errors:
            log_event("error", "bq_insert_error_detail", details=err)
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    log_event("info", "bq_insert_success", rows=len(rows))

# ────────────────────────────────────────────────────────────────
# Step 0 – Setup and mode detection
# ────────────────────────────────────────────────────────────────
def fetch_nfl_games(load_date=None):
    table_id = TABLE_SCHEDULE
    api_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForDate"
    headers = {
        "x-rapidapi-key": get_secret("Tank_Rapidapi"),
        "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }

    is_historical_run = (
        load_date is not None and
        datetime.strptime(load_date, "%Y-%m-%d").date() != datetime.now().date()
    )
    start_date = datetime.strptime(load_date, "%Y-%m-%d") if load_date else datetime.now()

    try:
        if not is_historical_run:
            any_inserted = False

            # Step 1 – Loop: today + next 2 days
            for offset in range(3):
                game_date = (start_date + timedelta(days=offset)).strftime("%Y%m%d")
                inserted = process_game_date(table_id, api_url, headers, game_date)
                if inserted:
                    any_inserted = True

            # Step 2 – Summary log for success/failure
            if any_inserted:
                log_event("info", "games_data_inserted")
            else:
                log_event("info", "no_new_games_found")

    except Exception as e:
        log_event("error", "nfl_games_job_failed", error=str(e))
    finally:
        log_event("info", "nfl_games_job_completed")



def process_game_date(table_id, api_url, headers, game_date):
    try:
        # Step 3a – Pull API data
        raw_games = fetch_games_for_date(api_url, headers, game_date)
        log_event("info", "games_fetched", game_date=game_date, count=len(raw_games))
        save_raw_response({"body": raw_games}, game_date, prefix="nfl_games")

        # Step 3b – Transform records (DataFrame)
        df = transform_game_records(raw_games)
        if df.empty:
            raise ValueError("No valid rows to process after transform.")

        # Step 3c – Deduplicate
        existing_ids = set(check_existing_records(table_id, "gameID", df["gameID"].tolist()))
        df_new = df[~df["gameID"].isin(existing_ids)]

        # Step 3d – Insert new records into BigQuery
        if not df_new.empty:
            try:
                insert_into_bigquery(table_id, df_new.to_dict(orient="records"))
                log_event("info", "games_inserted", game_date=game_date, inserted=len(df_new))
                return True  # Inserted new rows
            except Exception as e:
                log_event("error", "games_insert_error", game_date=game_date, error=str(e))
                raise
        else:
            log_event("info", "no_new_games", game_date=game_date)
            return False  # No new rows to insert

    except Exception as e:
        log_event("error", "games_fetch_or_insert_failed", game_date=game_date, error=str(e))
        return False



# ────────────────────────────────────────────────────────────────
# Step 4 – Fetch games for a single date from API
# ────────────────────────────────────────────────────────────────
def fetch_games_for_date(api_url, headers, game_date):
    query = {"gameDate": game_date}
    games = fetch_and_validate_api_data(api_url, headers, query)
    if not games or "body" not in games:
        raise ValueError(f"No valid response for date {game_date}")
    return games["body"]


# ────────────────────────────────────────────────────────────────
# Step 5 – Transform API records into BigQuery-ready DataFrame
# ────────────────────────────────────────────────────────────────
def transform_game_records(game_body):
    """
    Transform raw API records (list of dicts) into a DataFrame ready for BigQuery insertion.
    Handles types, serialization, missing columns, and BigQuery compatibility.
    Returns a DataFrame.
    """
    import pandas as pd

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
    # 2. Timestamp (gameTime_epoch) as string for BigQuery
    df["gameTime_epoch"] = pd.to_datetime(df["gameTime_epoch"].astype(float), unit="s")
    df["gameTime_epoch"] = df["gameTime_epoch"].astype(str)  # BQ expects string for TIMESTAMP
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
# Step 6 – Dedupe against existing BigQuery data
# ────────────────────────────────────────────────────────────────
def dedupe_new_games(table_id, game_rows):
    game_ids = [row["gameID"] for row in game_rows]
    existing_ids = check_existing_records(table_id, "gameID", game_ids)
    return filter_new_records(existing_ids, game_rows, "gameID")
