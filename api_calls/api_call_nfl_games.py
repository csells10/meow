import requests
from google.cloud import secretmanager
from datetime import datetime
from utils.helper import insert_into_bigquery, get_secret, check_existing_records, filter_new_records

def fetch_nfl_games():
    api_key = get_secret('Tank_Rapidapi')
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForDate"
    
    # Hardcoded date for testing purposes
    # querystring = {"gameDate": datetime.now().strftime('%Y%m%d')}
    querystring = {"gameDate": "20240908"}  # Example: Hardcoded date in YYYYMMDD format

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
        # Check if response status is 200 (OK)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch data: {response.status_code}, {response.text}")

    try:
        games = response.json()  # Attempt to parse the response as JSON
    except ValueError:
        raise ValueError(f"Response content is not valid JSON: {response.text}")

    if not isinstance(games, dict):
        raise TypeError(f"Expected JSON object, got {type(games).__name__}")

    # If the API returns an empty list or no 'body', handle it gracefully
    if 'body' not in games or not games['body']:
        print("No games found for the specified date.")
        return "No games to insert."

    game_ids = [game.get('gameID') for game in games['body']]
    table_id = 'nfl-stream-406420.League.schedule'

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

    rows_to_insert = filter_new_records(existing_game_ids, rows_to_insert, 'gameID')

    if rows_to_insert:
        insert_into_bigquery(table_id, rows_to_insert)
        return 'Data inserted successfully!'
    else:
        return 'No new games to insert.'
