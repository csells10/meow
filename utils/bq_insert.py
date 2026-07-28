from google.cloud import bigquery
from datetime import datetime

def insert_model_outcome(row: dict):
    client = bigquery.Client()
    table_id = "nfl-stream-406420.Analytics.game_model_outcomes"

    errors = client.insert_rows_json(table_id, [row])

    if errors:
        print(f"❌ Insert failed: {errors}")
    else:
        print("✅ Model outcome inserted")