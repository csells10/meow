# import json
# import os
import pandas as pd
import requests

from datetime import datetime
from google.cloud import bigquery
from utils.gcs import upload_file_to_gcs
from utils.logging_setup import log_event
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_team_records,
    filter_changed_team_records
)
from utils.response_helpers import save_raw_response

def fetch_nfl_scores(load_date=None):
    # Step 1: Determine game date using pandas
    if load_date is None:
        game_date = pd.Timestamp.now(tz="America/New_York").strftime("%Y-%m-%d")
    else:
        game_date = load_date  # Must be in "YYYY-MM-DD" format

    print(f"Fetching data for: {game_date}")

    # === Step 2: Fetch Game URLs for the Date ===
    # This step uses your utility functions to:
    # - Load the API key from secure storage
    # - Construct and send a request to the Tank01 NFL Scores API
    # - Validate and parse the response
    # - Log the data retrieval
    # - Save the raw API response to Google Cloud Storage for auditing/debugging

    api_key = get_secret("Tank_Rapidapi")
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLScoresOnly"
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }
    querystring = {
        "gameDate": game_date.replace("-", ""),
        "topPerformers": "false"
    }

    try:
        game_data = fetch_and_validate_api_data(url, headers, querystring, context="NFL Scores API Call")
        log_event("info", "fetched_score_urls", data_date=game_date)
        
        save_raw_response(game_data, game_date, prefix="nfl_scores")
    except (ValueError, TypeError) as e:
        log_event("error", "score_urls_fetch_failed", error=str(e), data_date=game_date)
        return

    if not game_data:
        log_event("warning", "no_score_urls_found", data_date=game_date)
        return

    game_urls = game_data.get("body", [])
    print(f"Found {len(game_urls)} game URLs")

    # Step 3: Expand and transform all games
    rows = []
    for game_id, game in game_data.get("body", {}).items():
        try:
            # Convert epoch to EST datetime
            epoch = float(game.get("gameTime_epoch", 0))
            dt = pd.to_datetime(epoch, unit="s").tz_localize("UTC").tz_convert("America/New_York")
            game_datetime_est = dt.strftime("%Y-%m-%d %H:%M")
            game_date_est = dt.strftime("%Y-%m-%d")

            # Extract points for reference
            homePts = pd.to_numeric(game.get("homePts", 0), errors="coerce")
            awayPts = pd.to_numeric(game.get("awayPts", 0), errors="coerce")

            # Line score details
            line_score = game.get("lineScore", {})
            for side in ["home", "away"]:
                data = line_score.get(side, {})
                row = {
                    "gameID": game_id,
                    "teamID": game.get(f"teamID{side.capitalize()}"),
                    "team_type": side,
                    "Q1": pd.to_numeric(data.get("Q1", 0), errors="coerce"),
                    "Q2": pd.to_numeric(data.get("Q2", 0), errors="coerce"),
                    "Q3": pd.to_numeric(data.get("Q3", 0), errors="coerce"),
                    "Q4": pd.to_numeric(data.get("Q4", 0), errors="coerce"),
                    "OT": pd.to_numeric(data.get("OT", 0), errors="coerce"),
                    "homePts": homePts,
                    "awayPts": awayPts,
                    "game_date_est": game_date_est,
                    "game_datetime_est": game_datetime_est,
                }
                rows.append(row)

        except Exception as e:
            print(f"❌ Failed to process game {game_id}: {e}")
            continue

    df = pd.DataFrame(rows)

    # Step 4: Validate row count
    expected = len(game_urls) * 2
    actual = len(df)
    if actual != expected:
        print(f"⚠️ Expected {expected} rows (2 per game), but got {actual}. Skipping insert.")
        return

    # Step 5: Insert into BigQuery
    client = bigquery.Client()
    table_id = "nfl-stream-406420.Scores.scores_dev"
    errors = client.insert_rows_json(table_id, df.to_dict(orient="records"))
    if errors:
        print(f"❌ BigQuery insert errors: {errors}")
    else:
        print(f"✅ Inserted {len(df)} rows into {table_id}")
