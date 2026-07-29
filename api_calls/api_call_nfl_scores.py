from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Optional

import pandas as pd
from google.cloud import bigquery

from utils.helper import fetch_and_validate_api_data, get_secret
from utils.logging_setup import log_event
from utils.response_helpers import save_raw_response


PROJECT = "nfl-stream-406420"
BQ_SOURCE = "Scores.scores_to_process"
SCORES_TARGET = f"{PROJECT}.Scores.scores"
SCORE_STATUS_TARGET = f"{PROJECT}.Scores.score_status"
API_HOST = (
    "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
)
API_URL = f"https://{API_HOST}/getNFLScoresOnly"
API_KEY = get_secret("Tank_Rapidapi")

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST,
}
BASE_QUERYSTRING = {
    "topPerformers": "false",
}
SCORE_COLUMNS = (
    "gameID",
    "teamID",
    "teamAbv",
    "team_type",
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "OT",
    "homePts",
    "awayPts",
    "game_date_est",
    "game_datetime_est",
)
FINAL_STATUS_VALUES = {"completed", "final"}


def fetch_scores_to_process(client: bigquery.Client) -> List[dict]:
    sql = f"""
        SELECT *
        FROM `{PROJECT}.{BQ_SOURCE}`
        ORDER BY gameDate DESC
    """
    return [dict(row) for row in client.query(sql).result()]


def insert_rows_bq(client: bigquery.Client, rows: List[dict]) -> None:
    errors = client.insert_rows_json(SCORES_TARGET, rows)
    if errors:
        log_event(
            "error",
            "scores_insert_failed",
            details=str(errors)[:250],
        )
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    log_event("info", "scores_inserted", rows=len(rows))


def mark_score_as_loaded(
    client: bigquery.Client,
    game_id: str,
) -> None:
    rows_to_insert = [{
        "game_id": game_id,
        "score_loaded": True,
    }]
    errors = client.insert_rows_json(SCORE_STATUS_TARGET, rows_to_insert)
    if errors:
        log_event(
            "error",
            "score_status_insert_failed",
            game_id=game_id,
            details=str(errors)[:250],
        )
        raise RuntimeError(f"Score-status insert errors: {errors}")
    log_event("info", "score_status_inserted", game_id=game_id)


def normalize_api_date(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")

    text = str(value or "").strip()
    for date_format in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10], date_format).strftime(
                "%Y%m%d"
            )
        except ValueError:
            continue
    raise ValueError(f"Unsupported game date: {value!r}")


def get_backlog_game_date(game: dict) -> str:
    for value in (
        game.get("gameDate"),
        game.get("game_date_est"),
        str(game.get("gameID") or "")[:8],
    ):
        if value:
            try:
                return normalize_api_date(value)
            except ValueError:
                continue
    raise ValueError(
        f"Could not determine API date for game {game.get('gameID')!r}"
    )


def _required_nonnegative_int(value, field_name: str) -> int:
    if value is None or value == "":
        raise ValueError(f"Missing score field: {field_name}")
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"Malformed score field {field_name}: {value!r}"
        )
    if number < 0:
        raise ValueError(
            f"Negative score field {field_name}: {number}"
        )
    return number


def _is_final_game(game_data: dict) -> bool:
    statuses = {
        str(game_data.get(field) or "").strip().lower()
        for field in ("gameStatus", "gameStatusCode", "period")
    }
    return bool(statuses & FINAL_STATUS_VALUES)


def build_score_rows(game_id: str, game_data: dict) -> List[dict]:
    if not game_id or not isinstance(game_data, dict):
        raise ValueError("Missing game identity or score payload")

    payload_game_id = game_data.get("gameID")
    if payload_game_id and str(payload_game_id) != str(game_id):
        raise ValueError(
            "Score payload game identity does not match requested game"
        )
    if not _is_final_game(game_data):
        raise ValueError("Game is not final")

    line_score = game_data.get("lineScore")
    if not isinstance(line_score, dict) or not line_score:
        raise ValueError("Missing lineScore")

    home_points = _required_nonnegative_int(
        game_data.get("homePts"),
        "homePts",
    )
    away_points = _required_nonnegative_int(
        game_data.get("awayPts"),
        "awayPts",
    )

    epoch = game_data.get("gameTime_epoch")
    try:
        game_datetime = pd.to_datetime(
            float(epoch),
            unit="s",
            utc=True,
        ).tz_convert("America/New_York")
    except (TypeError, ValueError, OverflowError):
        raise ValueError(f"Invalid gameTime_epoch: {epoch!r}")

    game_datetime_est = game_datetime.strftime("%Y-%m-%d %H:%M:%S")
    game_date_est = game_datetime.strftime("%Y-%m-%d")
    rows = []
    team_ids = set()

    for side in ("home", "away"):
        side_data = line_score.get(side)
        if not isinstance(side_data, dict) or not side_data:
            raise ValueError(f"Missing {side} lineScore")

        team_id = game_data.get(f"teamID{side.capitalize()}")
        if team_id is None or str(team_id).strip() == "":
            raise ValueError(f"Missing {side} team identity")
        team_id = str(team_id)
        if team_id in team_ids:
            raise ValueError("Home and away team identities must differ")
        team_ids.add(team_id)

        quarter_points = {
            quarter: _required_nonnegative_int(
                side_data.get(quarter),
                f"{side}.{quarter}",
            )
            for quarter in ("Q1", "Q2", "Q3", "Q4")
        }
        overtime_points = _required_nonnegative_int(
            side_data.get("OT", 0),
            f"{side}.OT",
        )
        line_total = _required_nonnegative_int(
            side_data.get("totalPts"),
            f"{side}.totalPts",
        )
        expected_total = home_points if side == "home" else away_points

        if line_total != expected_total:
            raise ValueError(
                f"{side} line total does not match game total"
            )
        if sum(quarter_points.values()) + overtime_points != line_total:
            raise ValueError(
                f"{side} quarter scores do not match line total"
            )

        rows.append({
            "gameID": str(game_id),
            "teamID": team_id,
            "teamAbv": side_data.get("teamAbv"),
            "team_type": side,
            **quarter_points,
            "OT": overtime_points,
            "homePts": home_points,
            "awayPts": away_points,
            "game_date_est": game_date_est,
            "game_datetime_est": game_datetime_est,
        })

    if len(rows) != 2 or {row["team_type"] for row in rows} != {
        "home",
        "away",
    }:
        raise ValueError("Score rows must contain one home and one away team")
    if any(set(row) != set(SCORE_COLUMNS) for row in rows):
        raise ValueError("Generated score rows do not match the table contract")
    return rows


def fetch_stored_game_rows(
    client: bigquery.Client,
    game_id: str,
) -> List[dict]:
    columns = ", ".join(SCORE_COLUMNS)
    sql = f"""
        SELECT {columns}
        FROM `{SCORES_TARGET}`
        WHERE gameID = @game_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "game_id",
                "STRING",
                game_id,
            )
        ]
    )
    return [
        dict(row)
        for row in client.query(
            sql,
            job_config=job_config,
        ).result()
    ]


def delete_game_rows(
    client: bigquery.Client,
    game_id: str,
) -> None:
    sql = f"""
        DELETE FROM `{SCORES_TARGET}`
        WHERE gameID = @game_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "game_id",
                "STRING",
                game_id,
            )
        ]
    )
    client.query(sql, job_config=job_config).result()
    log_event("info", "existing_game_scores_deleted", game_id=game_id)


def _canonical_value(field: str, value):
    if value is None:
        return None
    if field in {"Q1", "Q2", "Q3", "Q4", "OT", "homePts", "awayPts"}:
        return int(value)
    if field == "game_date_est":
        if isinstance(value, (date, datetime)):
            return value.strftime("%Y-%m-%d")
        return str(value)[:10]
    if field == "game_datetime_est":
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value).replace("T", " ")[:19]
    return str(value)


def _canonical_rows(rows: List[dict]) -> List[tuple]:
    return sorted(
        tuple(
            _canonical_value(field, row.get(field))
            for field in SCORE_COLUMNS
        )
        for row in rows
    )


def rows_are_complete_and_equal(
    stored_rows: List[dict],
    expected_rows: List[dict],
) -> bool:
    if len(stored_rows) != 2:
        return False
    if {row.get("team_type") for row in stored_rows} != {"home", "away"}:
        return False
    return _canonical_rows(stored_rows) == _canonical_rows(expected_rows)


def reconcile_game_rows(
    client: bigquery.Client,
    game_id: str,
    rows: List[dict],
) -> bool:
    stored_rows = fetch_stored_game_rows(client, game_id)
    if rows_are_complete_and_equal(stored_rows, rows):
        log_event(
            "info",
            "game_scores_already_current",
            game_id=game_id,
        )
        return False

    if stored_rows:
        delete_game_rows(client, game_id)

    insert_rows_bq(client, rows)
    confirmed_rows = fetch_stored_game_rows(client, game_id)
    if not rows_are_complete_and_equal(confirmed_rows, rows):
        raise RuntimeError(
            f"Stored score rows could not be confirmed for {game_id}"
        )
    return True


def fetch_nfl_scores(load_date: Optional[str] = None) -> int:
    log_event("info", "nfl_scores_job_started", load_date=load_date)
    bq = bigquery.Client(project=PROJECT)
    backlog = fetch_scores_to_process(bq)

    target_date = normalize_api_date(load_date) if load_date else None
    games_by_date: Dict[str, List[dict]] = defaultdict(list)
    rejected_before_fetch = 0

    for game in backlog:
        game_id = game.get("gameID")
        try:
            game_date = get_backlog_game_date(game)
        except ValueError as exc:
            rejected_before_fetch += 1
            log_event(
                "warning",
                "score_game_rejected",
                game_id=game_id,
                reason=str(exc),
            )
            continue
        if target_date and game_date != target_date:
            continue
        games_by_date[game_date].append(game)

    eligible_count = sum(len(games) for games in games_by_date.values())
    log_event(
        "info",
        "scores_backlog_loaded",
        backlog=len(backlog),
        eligible=eligible_count,
        rejected=rejected_before_fetch,
    )
    if not games_by_date:
        log_event("info", "no_scores_to_process")
        return 0

    success_count = 0
    rejected_count = rejected_before_fetch

    for game_date, games in games_by_date.items():
        querystring = {
            **BASE_QUERYSTRING,
            "gameDate": game_date,
        }
        try:
            response = fetch_and_validate_api_data(
                API_URL,
                HEADERS,
                querystring,
                context=game_date,
            )
            save_raw_response(
                response,
                game_date,
                prefix="nfl_scores",
            )
            response_games = response.get("body", {})
            if not isinstance(response_games, dict):
                raise ValueError("Scores response body is not a game mapping")
        except Exception as exc:
            rejected_count += len(games)
            log_event(
                "error",
                "scores_api_date_failed",
                game_date=game_date,
                games=len(games),
                error=str(exc)[:300],
            )
            continue

        for game in games:
            game_id = game.get("gameID")
            try:
                game_data = response_games.get(game_id)
                rows = build_score_rows(game_id, game_data)
                inserted = reconcile_game_rows(
                    bq,
                    game_id,
                    rows,
                )
                mark_score_as_loaded(bq, game_id)
                success_count += 1
                log_event(
                    "info",
                    "score_game_loaded",
                    game_id=game_id,
                    inserted=inserted,
                )
            except Exception as exc:
                rejected_count += 1
                log_event(
                    "error",
                    "score_game_failed",
                    game_id=game_id,
                    error=str(exc)[:300],
                )

    log_event(
        "info",
        "nfl_scores_job_complete",
        eligible=eligible_count,
        successful=success_count,
        rejected=rejected_count,
    )
    return success_count
