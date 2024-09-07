import requests
from google.cloud import secretmanager
from datetime import datetime, timedelta
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_records,
    filter_new_records
)

def fetch_nfl_teams():
    api_key = get_secret('Tank_Rapidapi')
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams"
    
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }

    table_id = 'nfl-stream-406420.League.teams_partitioned'

    # Define the range for fetching yesterday and today's data
    date_range = [-1, 0]  # -1 for yesterday, 0 for today
    start_date = datetime.now().date()

    # Loop through the two days: yesterday and today
    for day_offset in date_range:
        data_date = (start_date + timedelta(days=day_offset)).strftime('%Y-%m-%d')
        print(f"Fetching data for date: {data_date}")

        # Fetch team data for the specific date
        try:
            teams = fetch_and_validate_api_data(url, headers)
        except (ValueError, TypeError) as e:
            print(f"Error fetching team data for {data_date}: {e}")
            continue
        
        if not teams:
            print(f"No teams data found for {data_date}")
            continue

        # Prepare data for BigQuery (add 'dataDate' field)
        rows_to_insert = [{
            'teamID': team.get('teamID'),
            'teamAbv': team.get('teamAbv'),
            'teamCity': team.get('teamCity'),
            'teamName': team.get('teamName'),
            'conference': team.get('conference'),
            'conferenceAbv': team.get('conferenceAbv'),
            'division': team.get('division'),
            'wins': int(team.get('wins', 0)),
            'loss': int(team.get('loss', 0)),
            'tie': int(team.get('tie', 0)),
            'currentStreakResult': team.get('currentStreak', {}).get('result', ''),
            'currentStreakLength': int(team.get('currentStreak', {}).get('length', 0)),
            'byeWeek2023': team.get('byeWeeks', {}).get('2023', [None])[0],
            'byeWeek2022': team.get('byeWeeks', {}).get('2022', [None])[0],
            'byeWeek2024': team.get('byeWeeks', {}).get('2024', [None])[0],
            'rushYds': int(team.get('teamStats', {}).get('Rushing', {}).get('rushYds', 0)),
            'rushTD': int(team.get('teamStats', {}).get('Rushing', {}).get('rushTD', 0)),
            'passYds': int(team.get('teamStats', {}).get('Passing', {}).get('passYds', 0)),
            'passTD': int(team.get('teamStats', {}).get('Passing', {}).get('passTD', 0)),
            'defensiveTD': int(team.get('teamStats', {}).get('Defense', {}).get('defTD', 0)),
            'sacks': int(team.get('teamStats', {}).get('Defense', {}).get('sacks', 0)),
            'interceptions': int(team.get('teamStats', {}).get('Defense', {}).get('defensiveInterceptions', 0)),
            'fumblesRecovered': int(team.get('teamStats', {}).get('Defense', {}).get('fumblesRecovered', 0)),
            'fgMade': int(team.get('teamStats', {}).get('Kicking', {}).get('fgMade', 0)),
            'puntYds': int(team.get('teamStats', {}).get('Punting', {}).get('puntYds', 0)),
            'nflComLogo1': team.get('nflComLogo1'),
            'espnLogo1': team.get('espnLogo1'),
            'dataDate': str(data_date)  # Add the partition date field for each day
        } for team in teams['body']]

        # Check for existing records in BigQuery to avoid duplicates
        team_ids = [team['teamID'] for team in teams['body']]
        existing_team_ids = check_existing_records(table_id, 'teamID', team_ids, data_date)

        # Filter out records that already exist
        rows_to_insert = filter_new_records(existing_team_ids, rows_to_insert, 'teamID')

        # Insert the data into BigQuery
        if rows_to_insert:
            insert_into_bigquery(table_id, rows_to_insert)
            print(f"Inserted {len(rows_to_insert)} teams for {data_date} into BigQuery")
        else:
            print(f"No new teams to insert for {data_date}")

