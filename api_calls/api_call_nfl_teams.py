import requests
import pandas as pd
from datetime import datetime
import json
import time
from utils.logging_setup import log_event
from utils.helper import (
    insert_into_bigquery,
    get_secret,
    fetch_and_validate_api_data,
    check_existing_team_records,
    filter_changed_team_records
)
from deepdiff import DeepDiff

original_json_response = None  # Store original response for comparison

def save_raw_response(response, data_date):
    global original_json_response
    original_json_response = response

    filename = f"nfl_teams_raw_response_{data_date}.json"
    with open(filename, 'w') as f:
        json.dump(response, f, indent=4)
    log_event("info", "saved_raw_response", file=filename)

def validate_transformation(final_df, original_json):
    transformed_data = final_df.to_dict(orient='records')
    diff = DeepDiff(original_json, transformed_data, ignore_order=True)

    if diff:
        log_event("warning", "transformation_discrepancy", diff=str(diff))
    else:
        log_event("info", "transformation_valid")

def insert_with_retry(table_id, rows_to_insert, retries=3, delay=2):
    for attempt in range(retries):
        try:
            insert_into_bigquery(table_id, rows_to_insert)
            log_event("info", "bigquery_insert_success", row_count=len(rows_to_insert))
            break
        except Exception as e:
            log_event("error", "bigquery_insert_failed", error=str(e), attempt=attempt+1)
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
                log_event("info", "retrying_bigquery_insert", attempt=attempt+2)
            else:
                log_event("critical", "bigquery_insert_final_failure", retries=retries)
                raise

def fetch_nfl_teams(load_date=None):
    # === 1a. Initialization and Setup ===
    log_event("info", "nfl_teams_job_started")
    global original_json_response

    data_date = load_date or datetime.now().strftime('%Y-%m-%d')
    table_id = 'nfl-stream-406420.Teams.teams'

    # === 1b. API Configuration ===
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

    # === 2a. API Request ===
    try:
        teams = fetch_and_validate_api_data(url, headers, querystring)
        log_event("info", "fetched_team_data", data_date=data_date)
        save_raw_response(teams, data_date)
    except (ValueError, TypeError) as e:
        log_event("error", "team_data_fetch_failed", error=str(e), data_date=data_date)
        return

    if not teams:
        log_event("warning", "no_team_data_found", data_date=data_date)
        return

    # === 2b. Normalize Response to DataFrame ===
    teams_df = pd.json_normalize(teams['body'])
    team_count = teams_df['teamID'].nunique()
    log_event("info", "api_response_parsed", shape=str(teams_df.shape), team_count=team_count)

    teams_df['dataDate'] = data_date

    # === 3. Transform Sections: Static, Stats, Performers ===
    melted_dfs = process_static_fields(teams_df, data_date, team_count)
    static_df = pd.concat(melted_dfs)
    log_event("info", "static_fields_processed", shape=str(static_df.shape))

    team_stats_df = process_team_stats(teams['body'], data_date, team_count)
    log_event("info", "team_stats_processed", shape=str(team_stats_df.shape))

    top_performers_df = process_top_performers(teams['body'], data_date, team_count)
    log_event("info", "top_performers_processed", shape=str(top_performers_df.shape))

    # === 4. Merge All Sections into Final Dataset ===
    final_df = build_final_df(melted_dfs, team_stats_df, top_performers_df, teams_df, data_date)
    final_df['Value'] = pd.to_numeric(final_df['Value'], errors='coerce')
    final_df = final_df.where(pd.notnull(final_df), None)

    rows_to_insert = json.loads(final_df.to_json(orient='records'))

    # === 5. Compare Against Existing Records ===
    ###<------CHATGPT MADE THIS CHANGE
    existing_records = check_existing_team_records(table_id, 'teamID', data_date)
    filtered_rows = filter_changed_team_records(existing_records, rows_to_insert)

    # === 6. Insert Only New or Changed Rows ===
    if filtered_rows:
        log_event("info", "bigquery_payload_ready", row_count=len(filtered_rows), sample=filtered_rows[:5])
        insert_with_retry(table_id, filtered_rows)
    else:
        log_event("info", "no_changes_detected", data_date=data_date)

    # === 7. Complete Job ===
    log_event("info", "nfl_teams_job_completed")

# -- Helper Functions (unchanged except logging converted to log_event) --

def build_final_df(static_dfs, team_stats_df, top_performers_df, teams_df, data_date):
    metadata_fields = ['teamID', 'teamAbv', 'teamCity', 'teamName', 'conference', 'division', 'dataDate']
    team_metadata_df = teams_df[metadata_fields].drop_duplicates()
    combined_df = pd.concat(static_dfs + [team_stats_df, top_performers_df], ignore_index=True)

    final_df = pd.merge(combined_df, team_metadata_df, on=['teamID', 'dataDate'], how='inner')
    return final_df[[
        'teamID', 'teamAbv', 'teamCity', 'teamName', 'conference', 'division',
        'Level1', 'Level2', 'Value', 'PlayerID', 'dataDate'
    ]]

def process_static_fields(teams_df, data_date, team_count):
    configs = [
        {
            'fields': ['teamID', 'teamName', 'teamCity', 'conference', 'division', 'wins', 'loss'],
            'level1': 'Team Info'
        },
        {
            'fields': ['teamID', 'byeWeeks.2022', 'byeWeeks.2023', 'byeWeeks.2024'],
            'level1': 'Bye Weeks',
            'rename': {'byeWeeks.2022': '2022-Byes', 'byeWeeks.2023': '2023-Byes', 'byeWeeks.2024': '2024-Byes'}
        }
    ]

    melted_dfs = []
    for config in configs:
        fields = config['fields']
        level1 = config['level1']
        rename = config.get('rename', {})

        missing = [f for f in fields if f not in teams_df.columns]
        if missing:
            log_event("warning", "missing_fields", level1=level1, missing=missing)
            continue

        df = pd.melt(
            teams_df[fields],
            id_vars=['teamID'],
            var_name='Level2',
            value_name='Value'
        ).assign(Level1=level1, PlayerID=None, dataDate=data_date)

        if rename:
            df['Level2'] = df['Level2'].map(rename)

        if df['teamID'].nunique() != team_count:
            log_event("warning", "static_field_team_count_mismatch", level1=level1, found=df['teamID'].nunique(), expected=team_count)

        log_event("info", "static_field_processed", level1=level1, rows=len(df))
        melted_dfs.append(df)

    return melted_dfs

def process_team_stats(teams_df, data_date, team_count):
    team_stats = []
    log_event("info", "process_team_stats_start", team_count=len(teams_df))

    for team in teams_df:
        team_id = team.get("teamID")
        stats_block = team.get("teamStats", {})

        if not stats_block:
            log_event("warning", "missing_team_stats", teamID=team_id)
            continue

        for category, stats in stats_block.items():
            for stat_key, stat_val in stats.items():
                level2 = f"{category}.{stat_key}"
                try:
                    stat_val = float(stat_val)
                except (ValueError, TypeError):
                    log_event("warning", "non_numeric_stat", stat=level2, teamID=team_id)
                    stat_val = None

                team_stats.append({
                    "teamID": team_id,
                    "Level1": "Team Stats",
                    "Level2": level2,
                    "Value": stat_val,
                    "PlayerID": None,
                    "dataDate": data_date
                })

    df = pd.DataFrame(team_stats)
    if df['teamID'].nunique() != team_count:
        log_event("warning", "team_stats_count_mismatch", expected=team_count, actual=df['teamID'].nunique())

    log_event("info", "processed_team_stats", count=len(df))
    return df

def process_top_performers(teams_df, data_date, team_count):
    top_performers = []
    log_event("info", "process_top_performers_start", team_count=len(teams_df))

    for team in teams_df:
        team_id = team.get("teamID")
        performers = team.get("topPerformers", {})

        if not performers:
            log_event("warning", "missing_top_performers", teamID=team_id)
            continue

        for category, stats in performers.items():
            for stat_name, details in stats.items():
                value = details.get("total")
                player_ids = details.get("playerID", [])

                try:
                    value = float(value)
                except (ValueError, TypeError):
                    log_event("warning", "non_numeric_top_performer", stat=stat_name, teamID=team_id)
                    value = None

                top_performers.append({
                    "teamID": team_id,
                    "Level1": "Top Performers",
                    "Level2": stat_name,
                    "Value": value,
                    "PlayerID": player_ids[0] if player_ids else None,
                    "dataDate": data_date
                })

    df = pd.DataFrame(top_performers)
    if df['teamID'].nunique() != team_count:
        log_event("warning", "top_performers_count_mismatch", expected=team_count, actual=df['teamID'].nunique())

    log_event("info", "processed_top_performers", count=len(df))
    return df
