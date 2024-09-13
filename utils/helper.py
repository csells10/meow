import requests
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
    
def check_existing_today(table_id, date_column='date_column'):
    """
    Check if today's data already exists in the table.
    Returns the count of rows matching today's date.
    """
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
        SELECT COUNT(*) as count
        FROM `{table_id}`
        WHERE {date_column} = CURRENT_DATE()
    """
    
    try:
        query_job = client.query(query)
        result = query_job.result()
        
        # Fetch and return the count
        for row in result:
            return row["count"]
    except (GoogleAPICallError, NotFound) as e:
        print(f"Error querying BigQuery: {e}")
        return None  # None indicates an error occurred


def filter_new_records(existing_keys, records, key_column):
    """Filter out records that already exist in the table."""
    return [record for record in records if record[key_column] not in existing_keys]

def insert_into_bigquery(table_id, rows_to_insert):
    """Insert new records into BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        raise RuntimeError(f"Encountered errors while inserting rows: {errors}")
def fetch_and_validate_api_data(url, headers, querystring):
    """
    Fetches and validates data from an API, ensuring it's a valid JSON response.
    
    Parameters:
    - url: The API endpoint.
    - headers: The request headers (e.g., API key).
    - querystring: The query parameters for the API call.

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

    # Check if the 'body' field exists and contains data
    if 'body' not in data or not data['body']:
        print("No data found for the specified request.")
        return None  # You can return a more meaningful message or handle it differently

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
    print(f"Deleted records from {table_id} where gameDate = {yesterday}")