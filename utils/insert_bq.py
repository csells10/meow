from google.cloud import bigquery

from google.cloud import bigquery

def create_dev_scores_table():
    project_id = 'nfl-stream-406420'
    dataset_id = 'League'
    table_id = 'schedule_dev'

    client = bigquery.Client(project=project_id)

    schema = [
        bigquery.SchemaField("gameID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("seasonType", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("away", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gameDate", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("espnID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("teamIDHome", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gameStatus", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gameWeek", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("teamIDAway", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("home", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("espnLink", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cbsLink", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gameTime", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gameTime_epoch", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("season", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("neutralSite", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("gameStatusCode", "STRING", mode="NULLABLE"),
    ]

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)

    try:
        client.create_table(table)
        print(f"✅ Dev table created: {table_ref}")
    except Exception as e:
        print(f"⚠️ Failed to create table: {e}")

if __name__ == "__main__":
    create_dev_scores_table()
