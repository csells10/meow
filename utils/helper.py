import requests
import logging
import json
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import secretmanager
from google.api_core.exceptions import GoogleAPICallError, NotFound

PROJECT_ID = "nfl-stream-406420" 

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/nfl-stream-406420/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')

def check_existing_records(table_id, key_column, keys):
    """Check if the records with the given keys already exist in the table."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT {key_column}
        FROM `{table_id}`
        WHERE {key_column} IN UNNEST(@keys)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("keys", "STRING", keys)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    existing_keys = set(row[key_column] for row in query_job)
    return existing_keys
    
def check_existing_today(table_id, date_column='dataDate', check_date=None):
    """
    Check if data for a specific date exists in the table.
    If no check_date is provided, uses CURRENT_DATE().
    """
    client = bigquery.Client(project=PROJECT_ID)
    date_condition = f"{date_column} = DATE('{check_date}')" if check_date else f"{date_column} = CURRENT_DATE()"
    
    query = f"""
        SELECT COUNT(*) as count
        FROM `{table_id}`
        WHERE {date_condition}
    """

    try:
        query_job = client.query(query)
        result = query_job.result()
        for row in result:
            return row["count"] > 0
    except (GoogleAPICallError, NotFound) as e:
        print(f"Error querying BigQuery: {e}")
        return False  # Assume no data if there's an error
    
def filter_new_records(existing_keys, records, key_column):
    """Filter out records that already exist in the table."""
    return [record for record in records if record[key_column] not in existing_keys]

def insert_into_bigquery(table_id, rows_to_insert):
    """Insert new records into BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        raise RuntimeError(f"Encountered errors while inserting rows: {errors}")
def fetch_and_validate_api_data(url, headers, querystring, context=None):
    """
    Fetches and validates data from an API, ensuring it's a valid JSON response.
    
    Parameters:
    - url: The API endpoint.
    - headers: The request headers (e.g., API key).
    - querystring: The query parameters for the API call.
    - context: Optional string for logging/debugging context (e.g., which API or date)

    Returns:
    - Parsed JSON response if successful.
    
    Raises:
    - ValueError: If response status is not 200 or response is not valid JSON.
    - TypeError: If the expected response format is incorrect.
    """
    # Make the API request
    response = requests.get(url, headers=headers, params=querystring)

    # Check if response status is 200 (OK)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch data: {response.status_code}, {response.text}")

    try:
        data = response.json()  # Attempt to parse the response as JSON
    except ValueError:
        raise ValueError(f"Response content is not valid JSON: {response.text}")

    if not isinstance(data, dict):
        raise TypeError(f"Expected JSON object, got {type(data).__name__}")

    # TEMP: Print full response for debugging
    # print("Full API response:")
    # print(json.dumps(data, indent=2))

    # TEMP: Log full data for troubleshooting
    try:
        log_event("debug", "api_response_debug", context=context, raw_data=data)
    except Exception as e:
        print("Failed to log debug event:", e)

    # Check if the 'body' field exists and contains data
    if 'body' not in data or not data['body']:
        # Optionally raise an error or just return None
        # raise ValueError("Missing or empty 'body' in response")
        return None

    return data  # Return the parsed JSON

def delete_yesterdays_games_from_bigquery(table_id):
    """
    Deletes records from BigQuery table where gameDate is yesterday's date.

    Parameters:
    - table_id: The BigQuery table in 'project.dataset.table' format.
    """
    client = bigquery.Client(project=PROJECT_ID)
    
    # Calculate yesterday's date
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    query = f"""
    DELETE FROM `{table_id}`
    WHERE gameDate = '{yesterday}'
    """
    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete
    logging.info(f"Deleted records from {table_id} where gameDate = {yesterday}")
    
def check_existing_team_records(table_id, key_column, target_date):
    """
    Fetches existing team records for a specific date from BigQuery.
    Returns a dictionary: {(teamID, Level1, Level2, dataDate): full_record}
    """
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE dataDate = @target_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date)
        ]
    )

    query_job = client.query(query, job_config=job_config)

    existing_records = {}
    for row in query_job:
        key = (row["teamID"], row["Level1"], row["Level2"], row["dataDate"])
        existing_records[key] = dict(row)

    return existing_records

def filter_changed_team_records(existing_records, new_records):
    """
    Filters new_records to include only those that are new or have a different 'Value'.
    Uses composite key: (teamID, Level1, Level2, dataDate)
    """
    def make_key(r):
        return (r['teamID'], r['Level1'], r['Level2'], r['dataDate'])

    existing_by_key = existing_records
    changed_records = []

    for record in new_records:
        key = make_key(record)
        existing = existing_by_key.get(key)
        if not existing or str(record.get('Value')) != str(existing.get('Value')):
            changed_records.append(record)

    return changed_records