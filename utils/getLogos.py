import json
import os
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

def create_logo_table_if_needed(client, table_id):
    schema = [
        bigquery.SchemaField("teamID", "STRING", mode="REQUIRED", description="Unique team identifier"),
        bigquery.SchemaField("logoURL", "STRING", mode="REQUIRED", description="NFL.com team logo URL")
    ]

    table = bigquery.Table(table_id, schema=schema)
    try:
        client.create_table(table)
        print(f"✅ Created table `{table_id}`")
    except Conflict:
        print(f"⚠️ Table `{table_id}` already exists, skipping creation")

def load_team_logos_from_file(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)

    if "body" not in data:
        raise ValueError("Invalid structure: missing 'body' key")

    logo_records = [
        {
            "teamID": team["teamID"],
            "logoURL": team.get("nflComLogo1")
        }
        for team in data["body"]
        if team.get("nflComLogo1")
    ]

    df = pd.DataFrame(logo_records).drop_duplicates(subset=["teamID"])
    return df

def upload_logos_to_bigquery(df, table_id):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("teamID", "STRING"),
            bigquery.SchemaField("logoURL", "STRING"),
        ]
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} logo records to {table_id}")

def main():
    table_id = "nfl-stream-406420.Teams.team_logos"
    json_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'nfl_teams_raw_response_2025-06-23.json'))

    if not os.path.isfile(json_file):
        print(f"❌ File not found: {json_file}")
        return

    print(f"📁 Found logo source file: {json_file}")

    client = bigquery.Client()
    create_logo_table_if_needed(client, table_id)

    try:
        df = load_team_logos_from_file(json_file)
        if df.empty:
            print("⚠️ No logos found to upload.")
        else:
            print(f"🔍 Found {len(df)} logos. Uploading to BigQuery...")
            print(df.head())  # optional preview
            upload_logos_to_bigquery(df, table_id)
    except Exception as e:
        print(f"❌ Failed to process team logos: {e}")

if __name__ == "__main__":
    main()
