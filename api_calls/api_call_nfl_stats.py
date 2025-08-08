import os, time, json, requests, random
from typing import List

from api_calls.api_utils.parse_nfl_stats import parse_game_stats
from google.cloud import bigquery
from utils.helper import get_secret, fetch_and_validate_api_data
from utils.response_helpers import save_raw_response
from utils.logging_setup import log_event

PROJECT   = "nfl-stream-406420"
BQ_SOURCE = "League.games_to_process"
BQ_TARGET = "nfl-stream-406420.League.boxscore_status"
API_HOST  = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
API_URL   = f"https://{API_HOST}/getNFLBoxScore"
API_KEY   = get_secret("Tank_Rapidapi")

def fetch_games_to_process(client: bigquery.Client) -> List[dict]:
    sql = f"""
        SELECT *
        FROM `{PROJECT}.{BQ_SOURCE}`
        ORDER BY gameDate DESC
    """
    return [dict(r) for r in client.query(sql).result()]

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST,
}
BASE_QUERYSTRING = {
    "playByPlay": "false",
    "fantasyPoints": "false"
}

def insert_rows_bq(client: bigquery.Client, rows: List[dict]):
    log_event("debug", "bq_insert_start", rows=len(rows))
    errors = client.insert_rows_json(f"{PROJECT}.Analytics.game_metrics_flat", rows)
    if errors:
        log_event("error", "bq_insert_failed", details=str(errors)[:250])
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    log_event("info", "bq_insert_success", rows=len(rows))

def mark_game_as_loaded(client: bigquery.Client, game_id: str):
    rows_to_insert = [{
        "gameID": game_id,
        "boxscore_loaded": True,
    }]
    errors = client.insert_rows_json(BQ_TARGET, rows_to_insert)
    if errors:
        log_event("error", "boxscore_status_insert_failed", game_id=game_id, details=str(errors)[:250])
    else:
        log_event("info", "boxscore_status_inserted", game_id=game_id)

def fetch_nfl_stats():
    log_event("info", "nfl_stats_job_started")
    bq = bigquery.Client(project=PROJECT)

    backlog = fetch_games_to_process(bq)
    log_event("info", "games_to_ingest", count=len(backlog))
    success_count = 0

    for ix, g in enumerate(backlog, 1):
        game_id = g["gameID"]
        querystring = {**BASE_QUERYSTRING, "gameID": game_id}

        try:
            # 1. Fetch API data
            box = fetch_and_validate_api_data(API_URL, HEADERS, querystring, context=game_id)
            
            # 2. Save the raw API response to GCS (audit/backup)
            save_raw_response({"body": box}, game_id, prefix="nfl_boxscore")

            # 3. Parse and insert stats
            flat_rows = parse_game_stats(box)
            for r in flat_rows:
                r["gameID"] = game_id
            insert_rows_bq(bq, flat_rows)

            # 4. Insert flag into status table
            mark_game_as_loaded(bq, game_id)
            success_count += 1

            log_event("info", "game_loaded", game_id=game_id, rows=len(flat_rows), ix=ix, total=len(backlog))
        except Exception as e:
            log_event("error", "game_failed", game_id=game_id, error=str(e)[:300])

        # Jittered sleep between games
        time.sleep(0.7 + random.uniform(0, 0.3))

    log_event("info", "etl_job_complete", processed=len(backlog), successful=success_count)
    return success_count
