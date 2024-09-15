import requests
import pandas as pd
from datetime import datetime
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_today
)

def fetch_nfl_teams():
    api_key = get_secret('Tank_Rapidapi')
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams"
    
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }
    querystring = {"sortBy": "teamID", "rosters": "false", "schedules": "false", "topPerformers": "true", "teamStats": "true", "teamStatsSeason": "2023"}
    table_id = 'nfl-stream-406420.Teams.teams'

    # Get today's date
    data_date = datetime.now().strftime('%Y-%m-%d')
    print(f"Fetching data for date: {data_date}")

    # Check if today's data already exists in BigQuery
    if check_existing_today(table_id, date_column='dataDate'):
        print(f"Data for today ({data_date}) already exists. Skipping today's data.")
        return

    # Fetch team data for today
    try:
        teams = fetch_and_validate_api_data(url, headers, querystring)
    except (ValueError, TypeError) as e:
        print(f"Error fetching team data for {data_date}: {e}")
        return
    
    if not teams:
        print(f"No teams data found for {data_date}")
        return

    # Convert teams data into DataFrame
    teams_df = pd.json_normalize(teams['body'])

    # Add 'dataDate' field
    teams_df['dataDate'] = data_date

    # Define the team-level fields we need to extract
    team_info_fields = ['teamID', 'teamName', 'teamCity', 'conference', 'division', 'wins', 'loss']

    # Handle Team Info using `melt` to reshape
    team_info_df = pd.melt(
        teams_df[team_info_fields],
        id_vars=['teamID'],
        var_name='Level2',
        value_name='Value'
    ).assign(Level1='Team Info', PlayerID=None)

    # Handle Bye Weeks separately
    bye_weeks_df = pd.melt(
        teams_df[['teamID', 'byeWeeks.2022', 'byeWeeks.2023', 'byeWeeks.2024']],
        id_vars=['teamID'],
        var_name='Level2',
        value_name='Value'
    ).assign(Level1='Bye Weeks', PlayerID=None)
    
    # Clean up the "Level2" field for bye weeks
    bye_weeks_df['Level2'] = bye_weeks_df['Level2'].str.replace('byeWeeks.', '').str.cat(['-Byes'])

    # Process Team Stats
    team_stats_fields = {
        'RushingYards': 'teamStats.Rushing.rushYds',
        'RushingTouchdowns': 'teamStats.Rushing.rushTD',
        'PassingYards': 'teamStats.Passing.passYds',
        'PassingTouchdowns': 'teamStats.Passing.passTD',
        'KickingFieldGoalsMade': 'teamStats.Kicking.fgMade',
        'PuntingYards': 'teamStats.Punting.puntYds'
    }
    
    team_stats_df = pd.melt(
        teams_df[['teamID'] + list(team_stats_fields.values())],
        id_vars=['teamID'],
        var_name='Level2',
        value_name='Value'
    ).assign(Level1='Team Stats', PlayerID=None)
    
    # Clean up the "Level2" field for team stats
    team_stats_df['Level2'] = team_stats_df['Level2'].map({v: k for k, v in team_stats_fields.items()})

    # Handle Top Performers
    top_performer_stats = {
        'PassingAttempts': 'topPerformers.Passing.passAttempts.total',
        'PassingYards': 'topPerformers.Passing.passYds.total',
        'PassingTouchdowns': 'topPerformers.Passing.passTD.total',
        'RushingYards': 'topPerformers.Rushing.rushYds.total',
        'RushingTouchdowns': 'topPerformers.Rushing.rushTD.total',
        'ReceivingYards': 'topPerformers.Receiving.recYds.total',
        'ReceivingTouchdowns': 'topPerformers.Receiving.recTD.total',
        'KickingFieldGoalsMade': 'topPerformers.Kicking.fgMade.total'
    }

    top_performers_df = pd.melt(
        teams_df[['teamID'] + list(top_performer_stats.values())],
        id_vars=['teamID'],
        var_name='Level2',
        value_name='Value'
    ).assign(Level1='Top Performers', PlayerID=None)

    # Clean up the "Level2" field for top performers
    top_performers_df['Level2'] = top_performers_df['Level2'].map({v: k for k, v in top_performer_stats.items()})

    # Concatenate all the DataFrames
    final_df = pd.concat([team_info_df, bye_weeks_df, team_stats_df, top_performers_df])

    # Convert back to dictionary and insert into BigQuery
    rows_to_insert = final_df.to_dict(orient='records')

    # Insert the data into BigQuery
    if rows_to_insert:
        insert_into_bigquery(table_id, rows_to_insert)
        print(f"Inserted {len(rows_to_insert)} rows for {data_date} into BigQuery")
    else:
        print(f"No new teams to insert for {data_date}")
