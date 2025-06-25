from utils.logging_setup import log_event
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data
)
from datetime import datetime
import pandas as pd
import json

def fetch_nfl_scores(load_date=None):
    log_event("info", "new_game_data_fetch_started")

    api_key = get_secret("Tank_Rapidapi")
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForDate"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }

    game_date = load_date or datetime.now().strftime("%Y%m%d")
    querystring = {
        "gameDate": game_date,
        "topPerformers": "false"
    }

    try:
        response = fetch_and_validate_api_data(url, headers, querystring)
        games = response.get("body", [])
        log_event("info", "games_data_fetched", count=len(games))
    except Exception as e:
        log_event("error", "new_game_data_fetch_failed", error=str(e))
        return

    rows = []

    for game in games:
        game_time_epoch = game.get("gameTime_epoch")

        common_fields = {
            "gameID": game.get("gameID"),
            "gameTime_epoch": game_time_epoch,
            "homePts": game.get("homePts"),
            "awayPts": game.get("awayPts"),
        }

        for team_type in ["home", "away"]:
            team_data = game.get(team_type, {})
            row = {
                **common_fields,
                "teamID": team_data.get("teamID"),
                "Q1": team_data.get("Q1"),
                "Q2": team_data.get("Q2"),
                "Q3": team_data.get("Q3"),
                "Q4": team_data.get("Q4"),
            }
            rows.append(row)

    if rows:
        table_id = "nfl-stream-406420.Games.game_results"
        df = pd.DataFrame(rows)
        df = df.where(pd.notnull(df), None)  # Replace NaNs with None

        if "gameTime_epoch" in df.columns:
            df["game_datetime_est"] = pd.to_datetime(df["gameTime_epoch"].astype(float), unit='s', utc=True)
            df["game_datetime_est"] = df["game_datetime_est"].dt.tz_convert("America/New_York")
            df["game_date_est"] = df["game_datetime_est"].dt.date.astype(str)  # Partition field

        insert_rows = json.loads(df.to_json(orient="records"))

        try:
            insert_into_bigquery(table_id, insert_rows)
            log_event("info", "new_game_data_inserted", row_count=len(insert_rows))
        except Exception as e:
            log_event("error", "bigquery_insert_failed", error=str(e))
    else:
        log_event("warning", "no_game_data_to_insert", game_date=game_date)

    log_event("info", "new_game_data_fetch_completed")
