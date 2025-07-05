# api_call_nfl_box_scores_backfill.py
import json, pandas as pd
from typing import List
from google.cloud import bigquery
from utils.helper import get_secret, fetch_and_validate_api_data
from utils.logging_setup import log_event

PROJECT = "nfl-stream-406420"
BQ_SOURCE = "Scores.scores_to_process"
BQ_TARGET = "League.schedule"
API_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
API_URL = f"https://{API_HOST}/getNFLScoresOnly"
API_KEY = get_secret("Tank_Rapidapi")

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST,
}
BASE_QUERYSTRING = {
    "topPerformers": "false"
}

def fetch_scores_to_process(client: bigquery.Client) -> List[dict]:
    sql = f"""
        SELECT * FROM `{PROJECT}.{BQ_SOURCE}`
        ORDER BY gameDate DESC
    """
    return [dict(r) for r in client.query(sql).result()]

def insert_rows_bq(client: bigquery.Client, rows: List[dict]):
    errors = client.insert_rows_json(f"{PROJECT}.Scores.scores", rows)
    if errors:
        for err in errors:
            log_event("error", "bq_insert_error_detail", details=err)
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    log_event("info", "bq_insert_success", rows=len(rows))

def mark_game_as_loaded(client: bigquery.Client, game_id: str):
    sql = f"""
        UPDATE `{PROJECT}.{BQ_TARGET}`
        SET score_loaded = TRUE
        WHERE gameID = @game_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("game_id", "STRING", game_id)
        ]
    )
    client.query(sql, job_config=job_config).result()

def fetch_nfl_scores_backfill():
    log_event("info", "etl_start")
    bq = bigquery.Client(project=PROJECT)
    backlog = fetch_games_to_process(bq)
    log_event("info", "backlog_loaded", count=len(backlog))
    if not backlog:
        log_event("info", "no_scores_to_process")
        return

    for game in backlog:
        game_id = game["gameID"]
        querystring = {**BASE_QUERYSTRING, "gameID": game_id}
        try:
            response = fetch_and_validate_api_data(API_URL, HEADERS, querystring, context=game_id)
        except Exception as e:
            log_event("error", "api_call_failed", game_id=game_id, error=str(e))
            continue

        if not response:
            log_event("warning", "no_data_returned", game_id=game_id)
            continue

        game_data = response.get("body", {}).get(game_id)
        if not game_data:
            log_event("warning", "game_not_found_in_api", game_id=game_id)
            continue

        try:
            epoch = float(game_data.get("gameTime_epoch", 0))
            dt = pd.to_datetime(epoch, unit="s").tz_localize("UTC").tz_convert("America/New_York")
            game_datetime_est = dt.strftime("%Y-%m-%d %H:%M:%S")
            game_date_est = dt.strftime("%Y-%m-%d")
        except Exception as e:
            log_event("error", "datetime_conversion_failed", game_id=game_id, error=str(e))
            continue

        line_score = game_data.get("lineScore", {})
        rows = []
        for side in ["home", "away"]:
            data = line_score.get(side, {})
            row = {
                "gameID": game_id,
                "teamID": game_data.get(f"teamID{side.capitalize()}"),
                "teamAbv": data.get("teamAbv", ""),
                "team_type": side,
                "Q1": int(pd.to_numeric(data.get("Q1", 0), errors="coerce") or 0),
                "Q2": int(pd.to_numeric(data.get("Q2", 0), errors="coerce") or 0),
                "Q3": int(pd.to_numeric(data.get("Q3", 0), errors="coerce") or 0),
                "Q4": int(pd.to_numeric(data.get("Q4", 0), errors="coerce") or 0),
                "OT": int(pd.to_numeric(data.get("OT", 0), errors="coerce") or 0),
                "homePts": int(pd.to_numeric(game_data.get("homePts", 0), errors="coerce") or 0),
                "awayPts": int(pd.to_numeric(game_data.get("awayPts", 0), errors="coerce") or 0),
                "game_date_est": game_date_est,
                "game_datetime_est": game_datetime_est
            }
            rows.append(row)

        if len(rows) != 2:
            log_event("warning", "row_count_mismatch", game_id=game_id, expected=2, actual=len(rows), data=line_score)
            continue

        try:
            insert_rows_bq(bq, rows)
            mark_game_as_loaded(bq, game_id)
            log_event("info", "game_processed", game_id=game_id)
        except Exception as e:
            log_event("error", "bq_insert_or_mark_failed", game_id=game_id, error=str(e))

    log_event("info", "etl_job_complete")