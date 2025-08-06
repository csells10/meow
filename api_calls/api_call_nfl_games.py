import pandas as pd
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

# ────────────────────────────────────────────────────────────────
# Step 0 – Setup and mode detection
# ────────────────────────────────────────────────────────────────
def fetch_nfl_games(load_date=None):
    table_id = "nfl-stream-406420.League.schedule_dev"
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
            all_successful = False

            # ────────────────────────────────────────────────────────────────
            # Step 1 – Loop: today + next 2 days
            # ────────────────────────────────────────────────────────────────
            for offset in range(3):
                game_date = (start_date + timedelta(days=offset)).strftime("%Y%m%d")

                # Step 1a – Process the game date (fetch, transform, insert)
                successful = process_game_date(table_id, api_url, headers, game_date)
                all_successful = all_successful or successful

            # ────────────────────────────────────────────────────────────────
            # Step 2 – Summary log for success/failure
            # ────────────────────────────────────────────────────────────────
            if not all_successful:
                log_event("warning", "no_valid_games_found")
            else:
                log_event("info", "games_data_inserted")

    except Exception as e:
        log_event("error", "nfl_games_job_failed", error=str(e))
    finally:
        log_event("info", "nfl_games_job_completed")


def process_game_date(table_id, api_url, headers, game_date):
    try:
        # ───────────────────────────────────────
        # Step 3a – Pull API data
        # ───────────────────────────────────────
        raw_games = fetch_games_for_date(api_url, headers, game_date)
        log_event("info", "games_fetched", game_date=game_date, count=len(raw_games))
        save_raw_response({"body": raw_games}, game_date, prefix="nfl_games")

        # ───────────────────────────────────────
        # Step 3b – Transform records (pandas DataFrame)
        # ───────────────────────────────────────
        df = transform_game_records(raw_games)
        print(f"🧪 Transformed {len(df)} rows for {game_date}")

        if df.empty:
            raise ValueError("No valid rows to process after transform.")

        # ───────────────────────────────────────
        # Step 3c – Deduplicate
        # ───────────────────────────────────────
        existing_ids = set(check_existing_records(table_id, "gameID", df["gameID"].tolist()))
        print(f"🧪 Existing gameIDs in BQ: {existing_ids}")

        df_new = df[~df["gameID"].isin(existing_ids)]
        print(f"🧪 New rows to insert for {game_date}: {len(df_new)}")
        if not df_new.empty:
            print("🧪 First row sample:", df_new.iloc[0].to_dict())

        # ───────────────────────────────────────
        # Step 3d – Insert new records into BigQuery
        # ───────────────────────────────────────
        if not df_new.empty:
            try:
                insert_into_bigquery(table_id, df_new.to_dict(orient="records"))
                log_event("info", "games_inserted", game_date=game_date, inserted=len(df_new))
                print(f"✅ Inserted {len(df_new)} rows for {game_date}")
            except Exception as e:
                log_event("error", "games_insert_error", game_date=game_date, error=str(e))
                print("🛑 BQ insert failed:", str(e))
                raise
        else:
            log_event("info", "no_new_games", game_date=game_date)

        return True

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
# Step 5 – Transform API records into BigQuery rows
# ────────────────────────────────────────────────────────────────
def transform_game_records(game_body):
    import pandas as pd

    if not game_body:
        print("⚠️ No games to transform.")
        return []

    df = pd.DataFrame(game_body)
    print("Columns from API:", df.columns.tolist())  # ✅ Debug

    # Transform columns
    df["gameDate"] = pd.to_datetime(df["gameDate"], format="%Y%m%d").dt.date
    df["gameTime_epoch"] = pd.to_datetime(df["gameTime_epoch"].astype(float), unit="s")
    df["gameTime_epoch"] = df["gameTime_epoch"].apply(lambda x: x.to_pydatetime())
    df["neutralSite"] = df["neutralSite"].astype(str).str.lower() == "true"
    df["boxscore_loaded"] = False
    df["score_loaded"] = False

    required_cols = [
        "gameID", "seasonType", "away", "gameDate", "espnID", "teamIDHome",
        "gameStatus", "gameWeek", "teamIDAway", "home", "espnLink", "cbsLink",
        "gameTime", "gameTime_epoch", "season", "neutralSite", "gameStatusCode",
        "boxscore_loaded", "score_loaded"
    ]

    df = df.reindex(columns=required_cols)

    print("✅ Final transformed columns:", df.columns.tolist())
    print("✅ Sample row:", df.iloc[0].to_dict())
    # Ensure datetime objects are strings for BQ compatibility
    df["gameDate"] = df["gameDate"].apply(lambda x: x.strftime("%Y-%m-%d"))
    df["gameTime_epoch"] = df["gameTime_epoch"].astype(str)

    return df.to_dict(orient="records")



# ────────────────────────────────────────────────────────────────
# Step 6 – Dedupe against existing BigQuery data
# ────────────────────────────────────────────────────────────────
def dedupe_new_games(table_id, game_rows):
    game_ids = [row["gameID"] for row in game_rows]
    existing_ids = check_existing_records(table_id, "gameID", game_ids)
    return filter_new_records(existing_ids, game_rows, "gameID")
