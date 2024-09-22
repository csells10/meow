import logging
import requests
import pandas as pd
from datetime import datetime
import time
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_today
)

def fetch_nfl_teams(load_date=None):
    """
    Fetches NFL teams' data from an API and inserts it into BigQuery.
    Optionally allows deletion and reloading of data for a specified date.
    
    Args:
        load_date (str): Optional date string (YYYY-MM-DD). If provided, deletes and reloads data for this date.
                         If not provided, defaults to today's date.
    """

    # Step 1: Set dataDate as the provided load_date or today's date
    if load_date:
        data_date = load_date
    else:
        data_date = datetime.now().strftime('%Y-%m-%d')

    # Step 2: Retrieve API key from secret manager
    api_key = get_secret('Tank_Rapidapi')
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams"

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }
    querystring = {
        "sortBy": "teamID", 
        "rosters": "false", 
        "schedules": "false", 
        "topPerformers": "true", 
        "teamStats": "true", 
        "teamStatsSeason": "2023"
    }
    table_id = 'nfl-stream-406420.Teams.teams'

    # Step 3: Fetch the team data from the API (Single API Call)
    try:
        teams = fetch_and_validate_api_data(url, headers, querystring)
        logging.info(f"Fetched team data for {data_date}.")
    except (ValueError, TypeError) as e:
        logging.error(f"Error fetching team data for {data_date}: {e}", exc_info=True)
        return
    
    if not teams:
        logging.warning(f"No teams data found for {data_date}.")
        return

    # Step 4: Convert the API response into a DataFrame
    teams_df = pd.json_normalize(teams['body'])
    logging.info(f"Normalized DataFrame: {teams_df.head()}")  # Log the first few rows of the normalized DataFrame

    # Add 'dataDate' field for BigQuery partitioning
    teams_df['dataDate'] = data_date

    # Step 5: Process static fields (Team Info, Bye Weeks)
    melted_dfs = process_static_fields(teams_df, data_date)

    # Step 6: Dynamically process Team Stats
    team_stats_df = process_team_stats(teams_df, data_date)

    # Step 7: Dynamically process Top Performers (Index 0 only)
    top_performers_df = process_top_performers(teams_df, data_date)

    # Step 8: Concatenate all the processed DataFrames
    final_df = pd.concat([*melted_dfs, team_stats_df, top_performers_df])

    # Step 9: **Data Type Check** - Ensure that the 'Value' column is numeric
    final_df['Value'] = pd.to_numeric(final_df['Value'], errors='coerce')

    # Replace NaN values with None for compatibility with BigQuery
    final_df = final_df.where(pd.notnull(final_df), None)

    # Step 10: Log any NaN values in the 'Value' column
    nan_values = final_df[final_df['Value'].isna()]
    if not nan_values.empty:
        logging.warning(f"There are {len(nan_values)} rows with non-numeric 'Value' fields.")
        logging.debug(nan_values)  # Log the rows with NaN values for debugging

    # Step 11: Convert the DataFrame to a dictionary format and log the payload
    rows_to_insert = final_df.to_dict(orient='records')

    if rows_to_insert:
        logging.info(f"BigQuery payload: {rows_to_insert}")  # Log the payload for debugging
        insert_with_retry(table_id, rows_to_insert)
        logging.info(f"Inserted {len(rows_to_insert)} rows for {data_date} into BigQuery.")
    else:
        logging.info(f"No new teams to insert for {data_date}.")

def insert_with_retry(table_id, rows_to_insert, retries=3, delay=2):
    """
    Insert rows into BigQuery with retry logic.
    """
    for attempt in range(retries):
        try:
            insert_into_bigquery(table_id, rows_to_insert)
            logging.info(f"Successfully inserted {len(rows_to_insert)} rows into BigQuery.")
            break
        except Exception as e:
            logging.error(f"Error inserting into BigQuery: {e}", exc_info=True)
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
                logging.info(f"Retrying... (Attempt {attempt + 2}/{retries})")
            else:
                logging.critical(f"Failed to insert into BigQuery after {retries} attempts.")
                raise

# Processing Static Fields (Team Info, Bye Weeks)
def process_static_fields(teams_df, data_date):
    melt_configs = [
        {
            'fields': ['teamID', 'teamName', 'teamCity', 'conference', 'division', 'wins', 'loss'],
            'level1': 'Team Info',
            'rename': None  # No renaming needed for Team Info
        },
        {
            'fields': ['teamID', 'byeWeeks.2022', 'byeWeeks.2023', 'byeWeeks.2024'],
            'level1': 'Bye Weeks',
            'rename': {'byeWeeks.2022': '2022-Byes', 'byeWeeks.2023': '2023-Byes', 'byeWeeks.2024': '2024-Byes'}
        }
    ]

    melted_dfs = []  # Store processed DataFrames
    for config in melt_configs:
        fields = config['fields']
        level1 = config['level1']
        rename_mapping = config.get('rename', {})

        # Melt the DataFrame for this group
        df_melted = pd.melt(
            teams_df[fields],
            id_vars=['teamID'],
            var_name='Level2',
            value_name='Value'
        ).assign(Level1=level1, PlayerID=None, dataDate=data_date)

        # Apply renaming if specified
        if rename_mapping:
            df_melted['Level2'] = df_melted['Level2'].map(rename_mapping)

        melted_dfs.append(df_melted)
    
    return melted_dfs

# Ensure values are converted to floats only if they are numeric
def process_team_stats(teams_df, data_date):
    team_stats = []
    for _, team in teams_df.iterrows():
        team_id = team['teamID']
        stats_data = team.get('teamStats', {})

        for category, stats in stats_data.items():
            for stat_name, stat_value in stats.items():
                level2 = ''.join([word.capitalize() for word in stat_name.split()])

                # Ensure only numeric values are inserted into numeric fields
                try:
                    stat_value = float(stat_value)
                except ValueError:
                    stat_value = None  # Set to None if it's not numeric

                team_stats.append({
                    'teamID': team_id,
                    'Level1': 'Team Stats',
                    'Level2': level2,
                    'Value': stat_value,  # Ensure Value is numeric or None
                    'PlayerID': None,
                    'dataDate': data_date
                })

    return pd.DataFrame(team_stats)

# Ensure top performers' values are numeric where applicable
def process_top_performers(teams_df, data_date):
    top_performers = []

    for _, team in teams_df.iterrows():
        team_id = team['teamID']
        performers_data = team.get('topPerformers', {})

        for category, stats in performers_data.items():
            if 'total' in stats and len(stats['total']) > 0:
                level2 = ''.join([word.capitalize() for word in category.split()])

                # Ensure only numeric values are inserted into numeric fields
                try:
                    value = float(stats['total'][0])
                except ValueError:
                    value = None  # Set to None if it's not numeric

                top_performers.append({
                    'teamID': team_id,
                    'Level1': 'Top Performers',
                    'Level2': level2,
                    'Value': value,  # Ensure Value is numeric or None
                    'PlayerID': stats['playerID'][0] if len(stats['playerID']) > 0 else None,
                    'dataDate': data_date
                })

    return pd.DataFrame(top_performers)
