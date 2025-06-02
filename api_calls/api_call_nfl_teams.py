import logging
import requests
import pandas as pd
from datetime import datetime
import json
import time
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_today,
    check_existing_team_records
)
from deepdiff import DeepDiff

# Store the original response for comparison
original_json_response = None  # This will hold the original response for comparison


def save_raw_response(response, data_date):
    """
    Save the raw API response to a file for debugging and comparison.
    """
    global original_json_response
    original_json_response = response  # Store the original JSON for later comparison
    
    filename = f"nfl_teams_raw_response_{data_date}.json"
    with open(filename, 'w') as f:
        json.dump(response, f, indent=4)
    logging.info(f"Saved raw API response to {filename}")


def validate_transformation(final_df, original_json):
    """
    Validate that the final DataFrame matches the original JSON structure.
    Log any discrepancies.
    """
    # Re-convert the DataFrame to JSON-like structure for comparison
    transformed_data = final_df.to_dict(orient='records')
    
    # Perform a deep comparison between the original JSON and transformed DataFrame
    diff = DeepDiff(original_json, transformed_data, ignore_order=True)
    
    if diff:
        logging.warning(f"Discrepancies found between original JSON and transformed DataFrame: {diff}")
    else:
        logging.info("Transformation validation passed: No discrepancies found.")


def insert_with_retry(table_id, rows_to_insert, retries=3, delay=2):
    """
    Insert rows into BigQuery with retry logic.
    MOVE TO
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


def fetch_nfl_teams(load_date=None):
    """
    Fetches NFL teams' data from an API and inserts it into BigQuery.
    Optionally allows deletion and reloading of data for a specified date.
    
    Args:
        load_date (str): Optional date string (YYYY-MM-DD). If provided, deletes and reloads data for this date.
                         If not provided, defaults to today's date.
    """
    global original_json_response

    # Step 1: Set dataDate as the provided load_date or today's date
    if load_date:
        data_date = load_date
    else:
        data_date = datetime.now().strftime('%Y-%m-%d')
    
    # Step 2a: Check if data already exists, and delete if refreshing
    table_id = 'nfl-stream-406420.Teams.teams'  # ChatGPT: moved earlier to avoid undefined variable

    existing_team_ids = check_existing_team_records(table_id, 'teamID', data_date)
    if existing_team_ids:
        logging.info(f"Data already exists for {data_date} for teamIDs: {list(existing_team_ids)[:5]}... Skipping load.")
        return

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
        "teamStats": "true"
    }
    table_id = 'nfl-stream-406420.Teams.teams'

    # Step 3: Fetch the team data from the API (Single API Call)
    try:
        teams = fetch_and_validate_api_data(url, headers, querystring)
        logging.info(f"Fetched team data for {data_date}.")
        save_raw_response(teams, data_date)
    except (ValueError, TypeError) as e:
        logging.error(f"Error fetching team data for {data_date}: {e}", exc_info=True)
        return
    
    if not teams:
        logging.warning(f"No teams data found for {data_date}.")
        return

    # Step 4: Convert the API response into a DataFrame
    teams_df = pd.json_normalize(teams['body'])
    logging.info(f"API flatten response shape: {teams_df.shape}")
    # Determine the dynamic team count
    team_count = teams_df['teamID'].nunique()
    logging.info(f"Detected {team_count} unique teams in the API response.")
    
    # Add 'dataDate' field for BigQuery partitioning
    teams_df['dataDate'] = data_date

    # Step 5: Process static fields (Team Info, Bye Weeks)
    melted_dfs = process_static_fields(teams_df, data_date, team_count)
    
    # Log the combined static fields data
    static_df = pd.concat(melted_dfs)
    logging.info(f"Static fields processed. DataFrame head:\n{static_df.head()}")
    logging.info(f"Static DataFrame shape: {static_df.shape}")

    # Step 6: Dynamically process Team Stats
    team_stats_df = process_team_stats(teams['body'], data_date, team_count)
    
    # Log the team stats data
    logging.info(f"Team stats processed. DataFrame head:\n{team_stats_df.head()}")
    logging.info(f"Team Stats DataFrame shape: {team_stats_df.shape}")

    # Step 7: Dynamically process Top Performers (Index 0 only)
    top_performers_df = process_top_performers(teams['body'], data_date, team_count)
    
    # Log the top performers data
    logging.info(f"Top performers processed. DataFrame head:\n{top_performers_df.head()}")
    logging.info(f"Top Performers DataFrame shape: {top_performers_df.shape}")

    # Step 8: Combine all processed DataFrames
    
    # Old code
    # final_df = combine_all_data(melted_dfs, team_stats_df, top_performers_df, team_count)
    final_df = build_final_df(melted_dfs, team_stats_df, top_performers_df, teams_df, data_date)
    
    # Step 9: **Data Type Check** - Ensure that the 'Value' column is numeric
    final_df['Value'] = pd.to_numeric(final_df['Value'], errors='coerce')

    # Replace NaN values with None for compatibility with BigQuery
    final_df = final_df.where(pd.notnull(final_df), None)

    # Step 10: Log any NaN values in the 'Value' column
    # nan_values = final_df[final_df['Value'].isna()]
    # if not nan_values.empty:
    #     logging.warning(f"There are {len(nan_values)} rows with non-numeric 'Value' fields.")
    #     logging.debug(nan_values.head())  # Log a sample of rows with NaN values for debugging

    # Step 11: Log and validate the final DataFrame transformation
    # logging.info(f"Final DataFrame size: {len(final_df)} rows.")
    # validate_transformation(final_df, teams['body'])  # Compare transformed data with original JSON

    # Step 12: Convert the DataFrame to a dictionary format and log the payload
    final_df = final_df.where(pd.notnull(final_df), None)
    rows_to_insert = json.loads(final_df.to_json(orient='records'))

    if rows_to_insert:
        logging.info(f"BigQuery payload size: {len(rows_to_insert)} rows.")
        logging.info(f"First 5 rows of BigQuery payload: {rows_to_insert[:5]}")
        insert_with_retry(table_id, rows_to_insert)
        logging.info(f"Inserted {len(rows_to_insert)} rows for {data_date} into BigQuery.")
    else:
        logging.info(f"No new teams to insert for {data_date}.")


def old_combine_all_data(melted_dfs, team_stats_df, top_performers_df, team_count):
    """
    Combine all the processed DataFrames into one final DataFrame.
    This ensures that the team count remains consistent across all sections.
    """
    # Combine static fields DataFrame
    final_df = pd.concat(melted_dfs + [team_stats_df, top_performers_df], ignore_index=True)

    # Ensure consistency with the number of teams
    if final_df['teamID'].nunique() != team_count:
        logging.warning(f"Expected {team_count} teams in the final DataFrame, but found {final_df['teamID'].nunique()}.")

    # Log and inspect the final DataFrame
    logging.info(f"Final combined DataFrame has {len(final_df)} rows.")
    logging.info(f"First 50 rows of final combined DataFrame:\n{final_df.head()}")
    
    return final_df

def build_final_df(static_dfs, team_stats_df, top_performers_df, teams_df, data_date):
    # Select unique team metadata fields
    metadata_fields = ['teamID', 'teamAbv', 'teamCity', 'teamName', 'conference', 'division', 'dataDate']
    team_metadata_df = teams_df[metadata_fields].drop_duplicates()

    # Combine all value DataFrames
    combined_df = pd.concat(static_dfs + [team_stats_df, top_performers_df], ignore_index=True)

    # Join on teamID and dataDate
    final_df = pd.merge(combined_df, team_metadata_df, on=['teamID', 'dataDate'], how='inner')

    # Reorder columns to match BigQuery schema
    final_df = final_df[[
        'teamID', 'teamAbv', 'teamCity', 'teamName', 'conference', 'division',
        'Level1', 'Level2', 'Value', 'PlayerID', 'dataDate'
    ]]

    return final_df


def process_static_fields(teams_df, data_date, team_count):
    """
    Process static fields (like team info, bye weeks) from the original API response.
    This function also adds logging and validation to ensure fields are processed correctly.
    """
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

        # Ensure required fields exist in the DataFrame
        missing_fields = [field for field in fields if field not in teams_df.columns]
        if missing_fields:
            logging.warning(f"Missing expected fields in {level1}: {missing_fields}")
            continue

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

        # Check that the number of teams matches the initial count
        if df_melted['teamID'].nunique() != team_count:
            logging.warning(f"Expected {team_count} teams, but found {df_melted['teamID'].nunique()} in {level1}.")

        # Log the transformation result for inspection
        logging.info(f"Processed {len(df_melted)} rows for {level1}.")

        # Add the DataFrame to the list
        melted_dfs.append(df_melted)

    return melted_dfs

def process_team_stats(teams_df, data_date, team_count):
    """
    Process team stats directly from the raw JSON structure to retain original nesting.
    Includes detailed logging for debugging and validation.
    """
    team_stats = []

    logging.info("Starting Processing Team Stats from raw JSON")
    logging.info(f"Found {len(teams_df)} teams in input")

    for team in teams_df:
        team_id = team.get("teamID")
        team_abv = team.get("teamAbv")
        team_stat_block = team.get("teamStats", {})

        logging.info(f"Processing teamID {team_id} ({team_abv})")
        if not team_stat_block:
            logging.warning(f"No teamStats data found for teamID {team_id}")
            continue

        for category, stats in team_stat_block.items():
            for stat_key, stat_value in stats.items():
                level2 = f"{category}.{stat_key}"
                try:
                    stat_value = float(stat_value)
                except (ValueError, TypeError):
                    logging.warning(f"Non-numeric value for {level2} in teamID {team_id}")
                    stat_value = None

                team_stats.append({
                    "teamID": team_id,
                    "Level1": "Team Stats",
                    "Level2": level2,
                    "Value": stat_value,
                    "PlayerID": None,
                    "dataDate": data_date
                })

    df = pd.DataFrame(team_stats)

    if df['teamID'].nunique() != team_count:
        logging.warning(f"Expected {team_count} teams in Team Stats, but found {df['teamID'].nunique()}.")

    logging.info(f"Processed {len(team_stats)} team stats across {df['teamID'].nunique()} teams.")
    return df


def old_process_team_stats(teams_df, data_date, team_count):
    """
    Process team stats data from the API response.
    Adds validation and logging to ensure fields are processed correctly.
    """
    team_stats = []

    logging.info(f"Incoming dataframe shape: {teams_df.shape}")
    logging.info(f"Columns in Teams Dataframe {teams_df.columns.tolist()}")  # testing

    # Identify all teamStats columns dynamically
    team_stats_columns = [col for col in teams_df.columns if col.startswith('teamStats.')]

    # Iterate over each team in the DataFrame
    for _, team in teams_df.iterrows():
        team_id = team['teamID']
        team_name = team['teamAbv']

        # Log the teamID and abbreviation
        logging.info(f"Processing teamID {team_id}: teamAbv = {team_name}")

        # Iterate through each of the teamStats columns dynamically
        for stat_column in team_stats_columns:
            # Extract the value from the stat column
            stat_value = team[stat_column]

            # Validate if the value is numeric
            try:
                stat_value = float(stat_value)
            except (ValueError, TypeError):
                stat_value = None
                logging.warning(f"Non-numeric value found for {stat_column} in teamID {team_id}")

            # Add the stat to the list
            team_stats.append({
                'teamID': team_id,
                'Level1': 'Team Stats',
                'Level2': stat_column,  # Using the column name as Level2
                'Value': stat_value,
                'PlayerID': None,
                'dataDate': data_date
            })

        # Log the stats processed for the current team
        logging.info(f"Processed teamID {team_id} stats (first 5 entries): {team_stats[-5:]}")

    # Convert to DataFrame
    team_stats_df = pd.DataFrame(team_stats)

    # Log the size and head of the DataFrame
    logging.info(f"Team stats DataFrame shape: {team_stats_df.shape}")
    logging.info(f"Team stats DataFrame (head):\n{team_stats_df.head(10)}")

    # Check that the number of teams matches the initial count
    if team_stats_df['teamID'].nunique() != team_count:
        logging.warning(f"Expected {team_count} teams in Team Stats, but found {team_stats_df['teamID'].nunique()}.")

    # Log how many team stats were processed
    logging.info(f"Processed {len(team_stats)} team stats across {team_stats_df['teamID'].nunique()} teams.")
    
    return team_stats_df

import logging
import pandas as pd

def process_top_performers(teams_df, data_date, team_count):
    """
    Process top performers' data using raw JSON format to preserve nested structure.
    Adds detailed logging to track issues and transformations.
    """
    top_performers = []

    logging.info("Starting Processing Top Performers from raw JSON")
    logging.info(f"Found {len(teams_df)} teams in input")

    for team in teams_df:
        team_id = team.get("teamID")
        team_abv = team.get("teamAbv")

        performers_data = team.get("topPerformers", {})
        logging.info(f"Team {team_id} ({team_abv}) - Top Performers Keys: {list(performers_data.keys()) if performers_data else 'None'}")

        if not performers_data:
            logging.warning(f"No top performers data found for teamID {team_id}")
            continue

        for category, stats in performers_data.items():
            for stat_name, stat_details in stats.items():
                value = stat_details.get("total")
                player_ids = stat_details.get("playerID", [])

                try:
                    value = float(value)
                except (ValueError, TypeError):
                    logging.warning(f"Non-numeric value for {stat_name} in team {team_id}")
                    value = None

                top_performers.append({
                    "teamID": team_id,
                    "Level1": "Top Performers",
                    "Level2": stat_name,
                    "Value": value,
                    "PlayerID": player_ids[0] if isinstance(player_ids, list) and player_ids else None,
                    "dataDate": data_date
                })

    df = pd.DataFrame(top_performers)

    if df['teamID'].nunique() != team_count:
        logging.warning(f"Expected {team_count} teams in Top Performers, but found {df['teamID'].nunique()}.")

    logging.info(f"Processed {len(top_performers)} top performers.")
    return df



def old_process_top_performers(teams_df, data_date, team_count):
    """
    Process top performers' data from the API response.
    Adds validation and logging to ensure fields are processed correctly.
    """
    top_performers = []
    logging.info(f"Starting Processing Top Performers")
    logging.info(f"API flatten response shape: {teams_df.shape}")
    logging.info(f"Incoming files {teams_df.head(0)}")
    for _, team in teams_df.iterrows():
        team_id = team['teamID']
        logging.info(f'{team_id}')
        performers_data = team.get('topPerformers', {})
        logging.info(f"{performers_data}")

        if not performers_data:
            logging.warning(f"No top performers data found for teamID {team_id}")
            continue

        # Log the performers data for debugging
        logging.debug(f"Top performers for team {team_id}: {performers_data}")

        for category, stats in performers_data.items():
            for stat_name, stat_details in stats.items():
                if 'total' in stat_details and len(stat_details['total']) > 0:
                    level2 = ''.join([word.capitalize() for word in stat_name.split()])

                    # Ensure only numeric values are inserted into numeric fields
                    try:
                        value = float(stat_details['total'][0])
                    except (ValueError, TypeError):
                        value = None  # Set to None if it's not numeric

                    if value is None:
                        logging.warning(f"Non-numeric value found for {level2} in teamID {team_id}")

                    top_performers.append({
                        'teamID': team_id,
                        'Level1': 'Top Performers',
                        'Level2': level2,
                        'Value': value,  # Ensure Value is numeric or None
                        'PlayerID': stat_details['playerID'][0] if len(stat_details['playerID']) > 0 else None,
                        'dataDate': data_date
                    })

    top_performers_df = pd.DataFrame(top_performers)

    # Check that the number of teams matches the initial count
    if top_performers_df['teamID'].nunique() != team_count:
        logging.warning(f"Expected {team_count} teams in Top Performers, but found {top_performers_df['teamID'].nunique()}.")

    # Log how many top performers were processed
    logging.info(f"Processed {len(top_performers)} top performers.")
    
    return top_performers_df
