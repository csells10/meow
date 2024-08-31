import requests
from google.cloud import secretmanager
from datetime import datetime
from utils.helper import insert_into_bigquery

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/YOUR_PROJECT_ID/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')

def fetch_nfl_games():
    api_key = get_secret('my_api_key')
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForDate"
    querystring = {"gameDate": datetime.now().strftime('%Y%m%d')}  # dynamically set the game date

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    games = response.json()

    # Prepare data for BigQuery
    rows_to_insert = [{
        'gameID': game.get('gameID'),
        'seasonType': game.get('seasonType'),
        'away': game.get('away'),
        'gameDate': datetime.strptime(game.get('gameDate'), '%Y%m%d').date(),
        'espnID': game.get('espnID'),
        'teamIDHome': game.get('teamIDHome'),
        'gameStatus': game.get('gameStatus'),
        'gameWeek': game.get('gameWeek'),
        'teamIDAway': game.get('teamIDAway'),
        'home': game.get('home'),
        'espnLink': game.get('espnLink'),
        'cbsLink': game.get('cbsLink'),
        'gameTime': game.get('gameTime'),
        'gameTime_epoch': datetime.fromtimestamp(float(game.get('gameTime_epoch'))),
        'season': game.get('season'),
        'neutralSite': game.get('neutralSite') == 'True',
        'gameStatusCode': game.get('gameStatusCode')
    } for game in games] if games else []

    # Insert data into BigQuery
    table_id = 'YOUR_PROJECT_ID.YOUR_DATASET.YOUR_TABLE'
    insert_into_bigquery(table_id, rows_to_insert)

    return 'Data inserted successfully!'
