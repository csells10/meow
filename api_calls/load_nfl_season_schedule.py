import argparse
import json
import time
from datetime import date, timedelta
from typing import Iterable, Optional

from google.cloud import bigquery

from utils.helper import get_secret
from utils.logging_setup import log_event


PROJECT = "nfl-stream-406420"
TABLE_SCHEDULE = f"{PROJECT}.League.schedule"
API_URL = (
    "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/"
    "getNFLGamesForDate"
)


def current_nfl_season(today: Optional[date] = None) -> int:
    """Return the NFL season that contains the supplied calendar date."""
    today = today or date.today()
    return today.year if today.month >= 7 else today.year - 1


def season_date_range(season: int) -> tuple[date, date]:
    """Return a conservative date range covering one complete NFL season."""
    return date(season, 7, 1), date(season + 1, 3, 1) - timedelta(days=1)


def iter_dates(start_date: date, end_date: date) -> Iterable[date]:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def filter_games_for_season(
    games: list[dict],
    season: int,
) -> list[dict]:
    """Keep records by NFL season, not by the calendar year in gameDate."""
    season_text = str(season)
    return [
        game
        for game in games
        if str(game.get("season")) == season_text
    ]


def fetch_schedule_games(
    api_url: str,
    headers: dict,
    game_date: str,
) -> list[dict]:
    # Reuse the active schedule loader's API response handling without
    # changing its normal yesterday/today/next-two-days behavior.
    from api_calls.api_call_nfl_games import fetch_games_for_date

    return fetch_games_for_date(api_url, headers, game_date)


def transform_schedule_games(games: list[dict]):
    # Keep one schedule-table transformation contract.
    from api_calls.api_call_nfl_games import transform_game_records

    return transform_game_records(games)


def insert_schedule_rows(
    client: bigquery.Client,
    rows: list[dict],
) -> None:
    from api_calls.api_call_nfl_games import insert_rows_nfl_games

    insert_rows_nfl_games(client, rows)


def fetch_existing_game_ids(
    client: bigquery.Client,
    game_ids: list[str],
) -> set[str]:
    if not game_ids:
        return set()

    query = f"""
        SELECT gameID
        FROM `{TABLE_SCHEDULE}`
        WHERE gameID IN UNNEST(@game_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "game_ids",
                "STRING",
                game_ids,
            )
        ]
    )
    rows = client.query(query, job_config=job_config).result()
    return {row.gameID for row in rows}


def load_nfl_season_schedule(
    season: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    *,
    write: bool = False,
    sleep_seconds: float = 2.0,
    client: Optional[bigquery.Client] = None,
) -> dict:
    """
    Load missing schedule rows for one NFL season.

    The default run is a dry run. Existing gameIDs are never replaced, which
    preserves their score_loaded and boxscore_loaded flags.
    """
    season = season if season is not None else current_nfl_season()
    season = int(season)
    default_start, default_end = season_date_range(season)
    start_date = start_date or default_start
    end_date = end_date or default_end
    dates = list(iter_dates(start_date, end_date))

    client = client or bigquery.Client(project=PROJECT)
    headers = {
        "x-rapidapi-key": get_secret("Tank_Rapidapi"),
        "x-rapidapi-host": (
            "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        ),
    }
    summary = {
        "season": season,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "write": write,
        "dates_checked": 0,
        "failed_dates": [],
        "season_games_found": 0,
        "existing_games_skipped": 0,
        "games_to_insert": 0,
        "games_inserted": 0,
    }

    for index, current_date in enumerate(dates):
        game_date = current_date.strftime("%Y%m%d")
        summary["dates_checked"] += 1

        try:
            try:
                raw_games = fetch_schedule_games(API_URL, headers, game_date)
            except ValueError as exc:
                empty_response_message = (
                    f"No valid response for date {game_date}"
                )
                if str(exc) != empty_response_message:
                    raise
                raw_games = []

            season_games = filter_games_for_season(raw_games, season)
            summary["season_games_found"] += len(season_games)

            if not season_games:
                continue

            transformed = transform_schedule_games(season_games)
            rows = transformed.to_dict(orient="records")
            game_ids = [row["gameID"] for row in rows]
            existing_ids = fetch_existing_game_ids(client, game_ids)
            new_rows = [
                row
                for row in rows
                if row["gameID"] not in existing_ids
            ]

            summary["existing_games_skipped"] += (
                len(rows) - len(new_rows)
            )
            summary["games_to_insert"] += len(new_rows)

            if write and new_rows:
                insert_schedule_rows(client, new_rows)
                summary["games_inserted"] += len(new_rows)

        except Exception as exc:
            summary["failed_dates"].append(current_date.isoformat())
            log_event(
                "error",
                "nfl_season_schedule_date_failed",
                game_date=game_date,
                error=str(exc),
            )
        finally:
            dates_checked = index + 1
            if dates_checked % 25 == 0:
                print(
                    f"Progress: checked {dates_checked}/{len(dates)} dates"
                )
            if sleep_seconds > 0 and index < len(dates) - 1:
                time.sleep(sleep_seconds)

    log_event(
        "info",
        "nfl_season_schedule_complete",
        **summary,
    )
    return summary


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview or load missing NFL season schedule rows."
    )
    parser.add_argument(
        "--season",
        type=int,
        help="NFL season. Defaults dynamically from today's date.",
    )
    parser.add_argument("--start-date", type=parse_date)
    parser.add_argument("--end-date", type=parse_date)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Insert missing rows. Omit for a read-only preview.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=2.0,
        help="Delay between Tank01 requests (default: 2).",
    )
    args = parser.parse_args()

    summary = load_nfl_season_schedule(
        season=args.season,
        start_date=args.start_date,
        end_date=args.end_date,
        write=args.write,
        sleep_seconds=args.sleep_seconds,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
