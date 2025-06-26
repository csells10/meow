from google.cloud import bigquery

def create_production_table():
    project_id = 'nfl-stream-406420'
    dataset_id = 'Scores'
    table_id = 'scores'


    client = bigquery.Client(project=project_id)

    schema = [
        bigquery.SchemaField("gameID", "STRING", mode="REQUIRED", description="Unique game identifier, typically includes date and teams"),
        bigquery.SchemaField("teamID", "STRING", mode="REQUIRED", description="Team identifier (away or home)"),
        bigquery.SchemaField("teamAbv", "STRING", mode="NULLABLE", description="Team abbreviation, e.g., 'DAL' for Dallas"),
        bigquery.SchemaField("team_type", "STRING", mode="REQUIRED", description="'home' or 'away' to indicate team side"),
        bigquery.SchemaField("Q1", "INTEGER", mode="NULLABLE", description="Points scored in the 1st quarter"),
        bigquery.SchemaField("Q2", "INTEGER", mode="NULLABLE", description="Points scored in the 2nd quarter"),
        bigquery.SchemaField("Q3", "INTEGER", mode="NULLABLE", description="Points scored in the 3rd quarter"),
        bigquery.SchemaField("Q4", "INTEGER", mode="NULLABLE", description="Points scored in the 4th quarter"),
        bigquery.SchemaField("OT", "INTEGER", mode="NULLABLE", description="Points scored in overtime, if applicable"),
        bigquery.SchemaField("homePts", "INTEGER", mode="NULLABLE", description="Total points scored by the home team"),
        bigquery.SchemaField("awayPts", "INTEGER", mode="NULLABLE", description="Total points scored by the away team"),
        bigquery.SchemaField("game_date_est", "DATE", mode="REQUIRED", description="Game date in Eastern Time"),
        bigquery.SchemaField("game_datetime_est", "DATETIME", mode="NULLABLE", description="Game start date and time in Eastern Time, no timezone suffix"),
    ]

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)

    try:
        client.create_table(table)
        print(f"✅ Production table created: {table_ref}")
    except Exception as e:
        print(f"⚠️ Failed to create table: {e}")