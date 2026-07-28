import random
import time
from typing import List, Optional, Set

from api_calls.api_utils.parse_nfl_stats import parse_game_stats
from api_calls.api_utils.validate_nfl_boxscore import validate_nfl_boxscore
from google.cloud import bigquery
from utils.helper import get_secret, fetch_and_validate_api_data
from utils.response_helpers import save_raw_response
from utils.logging_setup import log_event

PROJECT = "nfl-stream-406420"
BQ_SOURCE = "League.games_to_process"
BQ_TARGET = "nfl-stream-406420.League.boxscore_status"
STATS_TARGET = f"{PROJECT}.Analytics.game_metrics_flat"
API_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
API_URL = f"https://{API_HOST}/getNFLBoxScore"
API_KEY = get_secret("Tank_Rapidapi")


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
    "fantasyPoints": "false",
}


def insert_rows_bq(client: bigquery.Client, rows: List[dict]):
    log_event("debug", "bq_insert_start", rows=len(rows))
    errors = client.insert_rows_json(STATS_TARGET, rows)
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
        log_event(
            "error",
            "boxscore_status_insert_failed",
            game_id=game_id,
            details=str(errors)[:250],
        )
        raise RuntimeError(f"Box-score status insert errors: {errors}")
    log_event("info", "boxscore_status_inserted", game_id=game_id)


def fetch_stored_team_ids(
    client: bigquery.Client,
    game_id: str,
) -> Set[str]:
    sql = f"""
        SELECT team_id
        FROM `{STATS_TARGET}`
        WHERE gameID = @game_id
        GROUP BY team_id
        HAVING COUNT(*) > 0
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("game_id", "STRING", game_id)
        ]
    )
    return {
        str(row["team_id"])
        for row in client.query(sql, job_config=job_config).result()
        if row["team_id"] is not None
    }


def delete_game_rows(client: bigquery.Client, game_id: str):
    sql = f"""
        DELETE FROM `{STATS_TARGET}`
        WHERE gameID = @game_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("game_id", "STRING", game_id)
        ]
    )
    client.query(sql, job_config=job_config).result()
    log_event("info", "existing_game_stats_deleted", game_id=game_id)


def confirm_parsed_teams(
    rows: List[dict],
    expected_team_ids: Set[str],
) -> None:
    parsed_team_ids = {
        str(row.get("team_id"))
        for row in rows
        if row.get("team_id") is not None
    }
    if not rows or parsed_team_ids != expected_team_ids:
        raise ValueError(
            "Parsed Stats rows do not represent both expected teams: "
            f"expected={sorted(expected_team_ids)}, "
            f"actual={sorted(parsed_team_ids)}"
        )


def reconcile_game_rows(
    client: bigquery.Client,
    game_id: str,
    rows: List[dict],
    expected_team_ids: Set[str],
) -> bool:
    stored_team_ids = fetch_stored_team_ids(client, game_id)
    if stored_team_ids == expected_team_ids:
        log_event(
            "info",
            "game_stats_already_complete",
            game_id=game_id,
            teams=len(stored_team_ids),
        )
        return False

    if stored_team_ids:
        delete_game_rows(client, game_id)

    insert_rows_bq(client, rows)
    confirmed_team_ids = fetch_stored_team_ids(client, game_id)
    if confirmed_team_ids != expected_team_ids:
        raise RuntimeError(
            "Stored Stats rows do not represent both expected teams: "
            f"expected={sorted(expected_team_ids)}, "
            f"actual={sorted(confirmed_team_ids)}"
        )
    return True


def fetch_nfl_stats(load_date: Optional[str] = None):
    log_event("info", "nfl_stats_job_started", load_date=load_date)
    bq = bigquery.Client(project=PROJECT)

    backlog = fetch_games_to_process(bq)
    log_event("info", "games_to_ingest", count=len(backlog))
    success_count = 0

    for ix, game in enumerate(backlog, 1):
        game_id = game["gameID"]
        querystring = {**BASE_QUERYSTRING, "gameID": game_id}

        try:
            box = fetch_and_validate_api_data(
                API_URL,
                HEADERS,
                querystring,
                context=game_id,
            )

            validation = validate_nfl_boxscore(
                box,
                expected_game_id=game_id,
            )
            if not validation.accepted:
                log_event(
                    "warning",
                    "game_stats_rejected",
                    game_id=game_id,
                    code=validation.code,
                    reason=validation.reason,
                )
                continue

            save_raw_response({"body": box}, game_id, prefix="nfl_boxscore")

            flat_rows = parse_game_stats(box)
            expected_team_ids = set(validation.team_ids)
            confirm_parsed_teams(flat_rows, expected_team_ids)
            for row in flat_rows:
                row["gameID"] = game_id

            inserted = reconcile_game_rows(
                bq,
                game_id,
                flat_rows,
                expected_team_ids,
            )
            mark_game_as_loaded(bq, game_id)
            success_count += 1

            log_event(
                "info",
                "game_loaded",
                game_id=game_id,
                rows=len(flat_rows),
                inserted=inserted,
                ix=ix,
                total=len(backlog),
            )
        except Exception as exc:
            log_event(
                "error",
                "game_failed",
                game_id=game_id,
                error=str(exc)[:300],
            )
        finally:
            time.sleep(0.7 + random.uniform(0, 0.3))

    log_event(
        "info",
        "etl_job_complete",
        processed=len(backlog),
        successful=success_count,
    )
    return success_count
