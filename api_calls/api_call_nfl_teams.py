import requests
from google.cloud import secretmanager
from datetime import datetime, timedelta
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_records,
    filter_new_records,
    check_existing_today
)

def fetch_nfl_teams():
    api_key = get_secret('Tank_Rapidapi')
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams"
    
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }
    querystring = {"sortBy":"teamID","rosters":"false","schedules":"false","topPerformers":"true","teamStats":"true","teamStatsSeason":"2023"}
    table_id = 'nfl-stream-406420.League.teams_partitioned'  # Adjusted table ID to match new schema

    # Get today's date
    data_date = datetime.now().strftime('%Y-%m-%d')
    print(f"Fetching data for date: {data_date}")

    # Fetch team data for today
    try:
        teams = fetch_and_validate_api_data(url, headers, querystring)
    except (ValueError, TypeError) as e:
        print(f"Error fetching team data for {data_date}: {e}")
        return
    
    if not teams:
        print(f"No teams data found for {data_date}")
        return

    # Prepare data for BigQuery (add 'dataDate' field)
    rows_to_insert = []
    
    for team in teams['body']:
        team_id = team.get('teamID')

        ### Level 1: Team Info (General Team Details)
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Info',
            'Level2': 'TeamName',
            'Value': team.get('teamName'),
            'PlayerID': None,
            'dataDate': data_date,
        })

        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Info',
            'Level2': 'City',
            'Value': team.get('teamCity'),
            'PlayerID': None,
            'dataDate': data_date,
        })

        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Info',
            'Level2': 'Conference',
            'Value': team.get('conference'),
            'PlayerID': None,
            'dataDate': data_date,
        })

        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Info',
            'Level2': 'Division',
            'Value': team.get('division'),
            'PlayerID': None,
            'dataDate': data_date,
        })

        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Info',
            'Level2': 'Wins',
            'Value': int(team.get('wins', 0)),
            'PlayerID': None,
            'dataDate': data_date,
        })

        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Info',
            'Level2': 'Losses',
            'Value': int(team.get('loss', 0)),
            'PlayerID': None,
            'dataDate': data_date,
        })

        ### Bye Weeks (Under Team Info)
        bye_weeks = team.get('byeWeeks', {})
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Bye Weeks',
            'Level2': '2022-Byes',
            'Value': bye_weeks.get('2022', [None])[0],
            'PlayerID': None,
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Bye Weeks',
            'Level2': '2023-Byes',
            'Value': bye_weeks.get('2023', [None])[0],
            'PlayerID': None,
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Bye Weeks',
            'Level2': '2024-Byes',
            'Value': bye_weeks.get('2024', [None])[0],
            'PlayerID': None,
            'dataDate': data_date,
        })

        ### Level 1: Team Stats
        team_stats = team.get('teamStats', {})
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Stats',
            'Level2': 'RushingYards',
            'Value': int(team_stats.get('Rushing', {}).get('rushYds', 0)),
            'PlayerID': None,
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Stats',
            'Level2': 'RushingTouchdowns',
            'Value': int(team_stats.get('Rushing', {}).get('rushTD', 0)),
            'PlayerID': None,
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Stats',
            'Level2': 'PassingYards',
            'Value': int(team_stats.get('Passing', {}).get('passYds', 0)),
            'PlayerID': None,
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Stats',
            'Level2': 'PassingTouchdowns',
            'Value': int(team_stats.get('Passing', {}).get('passTD', 0)),
            'PlayerID': None,
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Stats',
            'Level2': 'KickingFieldGoalsMade',
            'Value': int(team_stats.get('Kicking', {}).get('fgMade', 0)),
            'PlayerID': None,
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Team Stats',
            'Level2': 'PuntingYards',
            'Value': int(team_stats.get('Punting', {}).get('puntYds', 0)),
            'PlayerID': None,
            'dataDate': data_date,
        })

        ### Level 1: Top Performers (with PlayerIDs)
        top_performers = team.get('topPerformers', {})

        # Passing Top Performer
        passing = top_performers.get('Passing', {})
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Top Performers',
            'Level2': 'PassingAttempts',
            'Value': int(passing.get('passAttempts', {}).get('total', 0)),
            'PlayerID': passing.get('passAttempts', {}).get('playerID', [None])[0],
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Top Performers',
            'Level2': 'PassingYards',
            'Value': int(passing.get('passYds', {}).get('total', 0)),
            'PlayerID': passing.get('passYds', {}).get('playerID', [None])[0],
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Top Performers',
            'Level2': 'PassingTouchdowns',
            'Value': int(passing.get('passTD', {}).get('total', 0)),
            'PlayerID': passing.get('passTD', {}).get('playerID', [None])[0],
            'dataDate': data_date,
        })

        # Rushing Top Performer
        rushing = top_performers.get('Rushing', {})
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Top Performers',
            'Level2': 'RushingYards',
            'Value': int(rushing.get('rushYds', {}).get('total', 0)),
            'PlayerID': rushing.get('rushYds', {}).get('playerID', [None])[0],
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Top Performers',
            'Level2': 'RushingTouchdowns',
            'Value': int(rushing.get('rushTD', {}).get('total', 0)),
            'PlayerID': rushing.get('rushTD', {}).get('playerID', [None])[0],
            'dataDate': data_date,
        })

        # Receiving Top Performer
        receiving = top_performers.get('Receiving', {})
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Top Performers',
            'Level2': 'ReceivingYards',
            'Value': int(receiving.get('recYds', {}).get('total', 0)),
            'PlayerID': receiving.get('recYds', {}).get('playerID', [None])[0],
            'dataDate': data_date,
        })
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Top Performers',
            'Level2': 'ReceivingTouchdowns',
            'Value': int(receiving.get('recTD', {}).get('total', 0)),
            'PlayerID': receiving.get('recTD', {}).get('playerID', [None])[0],
            'dataDate': data_date,
        })

        # Kicking Top Performer
        kicking = top_performers.get('Kicking', {})
        rows_to_insert.append({
            'teamID': team_id,
            'Level1': 'Top Performers',
            'Level2': 'KickingFieldGoalsMade',
            'Value': int(kicking.get('fgMade', {}).get('total', 0)),
            'PlayerID': kicking.get('fgMade', {}).get('playerID', [None])[0],
            'dataDate': data_date,
        })

    # Insert the data into BigQuery
    if rows_to_insert:
        insert_into_bigquery(table_id, rows_to_insert)
        print(f"Inserted {len(rows_to_insert)} rows for {data_date} into BigQuery")
    else:
        print(f"No new teams to insert for {data_date}")