# api_call_nfl_box_score.py
import os, time, json, requests, random
from typing import List

from api_calls.api_utils.parse_game_stats import parse_game_stats
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from utils.helper import get_secret, fetch_and_validate_api_data
from utils.logging_setup import log_event

PROJECT   = "nfl-stream-406420"
BQ_SOURCE = "League.games_to_process"
BQ_TARGET = "League.schedule"                 
API_HOST  = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
API_URL   = f"https://{API_HOST}/getNFLBoxScore"
API_KEY   = get_secret("Tank_Rapidapi")


# ─────────────────────────────────────────────
# 1) Pull gameIDs still missing from BigQuery
# ─────────────────────────────────────────────
def fetch_games_to_process(client: bigquery.Client) -> List[dict]:
    sql = f"""
        SELECT
            *
        FROM `{PROJECT}.{BQ_SOURCE}`
        ORDER BY gameDate DESC
    """
    return [dict(r) for r in client.query(sql).result()]

# ─────────────────────────────────────────────
# 2) Call Tank01 API
# ─────────────────────────────────────────────

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST,
}
BASE_QUERYSTRING = {
    "playByPlay": "false",
    "fantasyPoints": "false"
}

# ─────────────────────────────────────────────
# 3) Load to BigQuery
# ─────────────────────────────────────────────
def insert_rows_bq(client: bigquery.Client, rows: List[dict]):
    log_event("debug", "bq_insert_start", rows=len(rows))
    errors = client.insert_rows_json(f"nfl-stream-406420.Analytics.game_metrics_flat", rows)
    if errors:                                   # non-empty list => errors
        log_event("error", "bq_insert_failed", details=str(errors)[:250])
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    log_event("info", "bq_insert_success", rows=len(rows))

# ─────────────────────────────────────────────
# 4) Mark Game ID as loaded 
# ─────────────────────────────────────────────
def mark_game_as_loaded(client: bigquery.Client, game_id: str):
    sql = f"""
        UPDATE `{PROJECT}.League.schedule`
        SET boxscore_loaded = TRUE
        WHERE gameID = @game_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("game_id", "STRING", game_id)
        ]
    )
    client.query(sql, job_config=job_config).result()

# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────
def main():
    bq = bigquery.Client(project=PROJECT)

    # Ensure the target table exists
    try:
        bq.get_table(f"{PROJECT}.{BQ_TARGET}")
    except NotFound:
        schema = [
            bigquery.SchemaField("team_id", "STRING"),
            bigquery.SchemaField("team_abv", "STRING"),
            bigquery.SchemaField("data_date", "DATE"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("metric", "STRING"),
            bigquery.SchemaField("core_area", "STRING"),
            bigquery.SchemaField("value", "FLOAT"),
            bigquery.SchemaField("gameID", "STRING"),
        ]
        bq.create_table(bigquery.Table(f"{PROJECT}.{BQ_TARGET}", schema=schema))

    backlog = fetch_games_to_process(bq)
    print(f"🗂️  {len(backlog)} games to ingest")

    for ix, g in enumerate(backlog, 1):
        game_id, game_date = g["gameID"], g["gameDate"]
        querystring = {**BASE_QUERYSTRING, "gameID": game_id}

        try:
            # Fetch and parse box score data
            box = fetch_and_validate_api_data(API_URL, HEADERS, querystring, context=game_id)
            flat_rows = parse_game_stats(box)
            for r in flat_rows:
                r["gameID"] = game_id
            insert_rows_bq(bq, flat_rows)
            mark_game_as_loaded(bq, game_id)

            print(f"✅ {ix}/{len(backlog)}  {game_id} inserted and marked as loaded ({len(flat_rows)} rows)")
            log_event("info", "game_loaded", game_id=game_id, rows=len(flat_rows))
        except Exception as e:
            print(f"❌ {game_id} failed: {e}")
            log_event("error", "game_failed", game_id=game_id, error=str(e)[:300])
        
        time.sleep(0.7 + random.uniform(0, 0.3))  # jitter to be safe

    log_event("info", "etl_job_complete", processed=len(backlog))


if __name__ == "__main__":
    main()
