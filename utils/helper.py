from google.cloud import bigquery
from google.cloud import secretmanager

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/nfl-stream-406420/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')

def check_existing_records(table_id, key_column, keys):
    """Check if the records with the given keys already exist in the table."""
    client = bigquery.Client()
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

def filter_new_records(existing_keys, records, key_column):
    """Filter out records that already exist in the table."""
    return [record for record in records if record[key_column] not in existing_keys]

def insert_into_bigquery(table_id, rows_to_insert):
    """Insert new records into BigQuery."""
    client = bigquery.Client()
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        raise RuntimeError(f"Encountered errors while inserting rows: {errors}")

