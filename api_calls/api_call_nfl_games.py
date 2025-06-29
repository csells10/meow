from datetime import datetime, timedelta
import requests
from utils.logging_setup import log_event
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_records,
    filter_new_records,
    delete_yesterdays_games_from_bigquery
)
from utils.response_helpers import save_raw_response

def fetch_nfl_games():
    log_event("info", "nfl_games_job_started")

    api_key = get_secret('Tank_Rapidapi')
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForDate"
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }

    table_id = 'nfl-stream-406420.League.schedule'
    start_date = datetime.now().date()

    # Step 1: Pre-fetch and validate yesterday's data
    yesterday_date = (start_date + timedelta(days=-1)).strftime('%Y%m%d')
    yesterday_data = None
    try:
        log_event("info", "prefetch_yesterday_games", game_date=yesterday_date)
        querystring = {"gameDate": yesterday_date}
        response = fetch_and_validate_api_data(url, headers, querystring)
        if response and 'body' in response:
            yesterday_data = response['body']
            log_event("info", "yesterday_games_fetched", count=len(yesterday_data))

            # Save raw response
            save_raw_response(response, yesterday_date, prefix="nfl_games")
        else:
            log_event("warning", "yesterday_games_fetch_empty", game_date=yesterday_date)
    except Exception as e:
        log_event("error", "yesterday_games_fetch_failed", game_date=yesterday_date, error=str(e))

    # Step 2: Only delete if yesterday's data is pre-fetched successfully
    if yesterday_data:
        log_event("info", "delete_yesterday_games")
        delete_yesterdays_games_from_bigquery(table_id)
    else:
        log_event("warning", "yesterday_data_not_deleted_due_to_fetch_failure", game_date=yesterday_date)

    # Step 3: Insert yesterday's pre-fetched data if available
    if yesterday_data:
        game_ids = [game.get('gameID') for game in yesterday_data]
        existing_game_ids = check_existing_records(table_id, 'gameID', game_ids)
        rows_to_insert = filter_new_records(existing_game_ids, yesterday_data, 'gameID')

        if rows_to_insert:
            insert_into_bigquery(table_id, rows_to_insert)
            log_event("info", "games_inserted", game_date=yesterday_date, inserted=len(rows_to_insert))
        else:
            log_event("info", "no_new_games", game_date=yesterday_date)

    # Step 4: Continue with today + next days
    days_range = 4
    any_valid_data = False
    failed_dates = []

    for day_offset in range(0, days_range - 1):
        game_date = (start_date + timedelta(days=day_offset)).strftime('%Y%m%d')
        querystring = {"gameDate": game_date}

        try:
            games = fetch_and_validate_api_data(url, headers, querystring)
            if not games or 'body' not in games:
                failed_dates.append(game_date)
                continue

            game_body = games.get('body', [])
            log_event("info", "games_fetched", game_date=game_date, count=len(game_body))

            # Save raw response
            save_raw_response(games, game_date, prefix="nfl_games")

        except (ValueError, TypeError) as e:
            log_event("error", "games_fetch_failed", game_date=game_date, error=str(e))
            failed_dates.append(game_date)
            continue

        any_valid_data = True

        game_ids = [game.get('gameID') for game in game_body]
        existing_game_ids = check_existing_records(table_id, 'gameID', game_ids)
        rows_to_insert = filter_new_records(existing_game_ids, game_body, 'gameID')

        if rows_to_insert:
            insert_into_bigquery(table_id, rows_to_insert)
            log_event("info", "games_inserted", game_date=game_date, inserted=len(rows_to_insert))
        else:
            log_event("info", "no_new_games", game_date=game_date)

    if not any_valid_data:
        log_event("warning", "no_valid_games_found", failed_dates=failed_dates)
    else:
        log_event("info", 'Data inserted successfully!')
        log_event("info", "nfl_games_job_completed")