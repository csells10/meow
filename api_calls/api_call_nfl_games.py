import logging
import requests
from google.cloud import secretmanager
from datetime import datetime, timedelta
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_records,
    filter_new_records,
    delete_yesterdays_games_from_bigquery
)

def fetch_nfl_games():
    logging.info("Starting NFL games fetch job...")

    api_key = get_secret('Tank_Rapidapi')
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForDate"

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }

    table_id = 'nfl-stream-406420.League.schedule'
    start_date = datetime.now().date()

    # Step 1: Delete yesterday's data
    logging.info("Deleting yesterday's games from BigQuery...")
    delete_yesterdays_games_from_bigquery(table_id)

    days_range = 4
    any_valid_data = False

    for day_offset in range(-1, days_range - 1):
        game_date = (start_date + timedelta(days=day_offset)).strftime('%Y%m%d')
        logging.info(f"Fetching games for date: {game_date}")
        querystring = {"gameDate": game_date}

        try:
            games = fetch_and_validate_api_data(url, headers, querystring)
            if not games or 'body' not in games:
                logging.warning(f"No valid response structure for {game_date}, skipping.")
                continue
            logging.info(f"Successfully fetched {len(games.get('body', []))} games for {game_date}")
        except (ValueError, TypeError) as e:
            logging.error(f"Error fetching data for {game_date}: {e}", exc_info=True)
            continue

        any_valid_data = True

        game_ids = [game.get('gameID') for game in games['body']]
        existing_game_ids = check_existing_records(table_id, 'gameID', game_ids)

        rows_to_insert = filter_new_records(existing_game_ids, rows_to_insert, 'gameID')

        if rows_to_insert:
            insert_into_bigquery(table_id, rows_to_insert)
            logging.info(f"Inserted {len(rows_to_insert)} games for {game_date}")
        else:
            logging.info(f"No new games to insert for {game_date}")

    if not any_valid_data:
        logging.warning("No valid games data found for any of the queried dates.")

    logging.info("NFL games fetch job completed successfully.")
    return 'Data inserted successfully!'

