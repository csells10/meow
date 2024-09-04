import requests
from google.cloud import secretmanager
from datetime import datetime, timedelta
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_records,
    filter_new_records,
    delete_old_games_from_bigquery
)

def fetch_nfl_games():
    api_key = get_secret('Tank_Rapidapi')
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForDate"
    
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }

    table_id = 'nfl-stream-406420.League.schedule'
    start_date = datetime.now().date()

    # Delete old records before fetching new data
    delete_old_games_from_bigquery(table_id, start_date)

    days_range = 3  # Fetch games for the next 3 days
    for day_offset in range(days_range):
        game_date = (start_date + timedelta(days=day_offset)).strftime('%Y%m%d')
        querystring = {"gameDate": game_date}
        
        # Fetch and validate API data using the helper function
        try:
            games = fetch_and_validate_api_data(url, headers, querystring)
        except (ValueError, TypeError) as e:
            print(f"Error fetching data for {game_date}: {e}")
            continue
        
        if not games:
            print(f"No games found for {game_date}")
            continue

        # Extract the game IDs
        game_ids = [game.get('gameID') for game in games['body']]
        
        # Check for existing records in BigQuery to avoid duplicates
        existing_game_ids = check_existing_records(table_id, 'gameID', game_ids)
        
        # Prepare data for BigQuery
        rows_to_insert = [{
            'gameID': game.get('gameID'),
            'seasonType': game.get('seasonType'),
            'away': game.get('away'),
            'gameDate': datetime.strptime(game.get('gameDate'), '%Y%m%d').strftime('%Y-%m-%d'),  # Convert to YYYY-MM-DD
            'espnID': game.get('espnID'),
            'teamIDHome': game.get('teamIDHome'),
            'gameStatus': game.get('gameStatus'),
            'gameWeek': game.get('gameWeek'),
            'teamIDAway': game.get('teamIDAway'),
            'home': game.get('home'),
            'espnLink': game.get('espnLink'),
            'cbsLink': game.get('cbsLink'),
            'gameTime': game.get('gameTime'),
            'gameTime_epoch': datetime.utcfromtimestamp(float(game.get('gameTime_epoch'))).isoformat(),
            'season': game.get('season'),
            'neutralSite': game.get('neutralSite') == 'True',
            'gameStatusCode': game.get('gameStatusCode')
        } for game in games['body']]
        
        # Filter out records that already exist
        rows_to_insert = filter_new_records(existing_game_ids, rows_to_insert, 'gameID')

        if rows_to_insert:
            insert_into_bigquery(table_id, rows_to_insert)
            print(f"Inserted {len(rows_to_insert)} games for {game_date}")
        else:
            print(f"No new games to insert for {game_date}")

    return 'Data inserted successfully!'
