from google.cloud import bigquery

def create_team_metrics_season_table():
    project_id = 'nfl-stream-406420'
    dataset_id = 'Analytics'
    table_id = 'team_metrics_season_2025'

    client = bigquery.Client(project=project_id)

    schema = [
        bigquery.SchemaField("season", "STRING", mode="REQUIRED", description="NFL season identifier (e.g., 2023)"),
        bigquery.SchemaField("data_date", "DATE", mode="REQUIRED", description="Calendar date of the last game included in this rollup"),
        bigquery.SchemaField("team_id", "STRING", mode="REQUIRED", description="Unique team identifier"),
        bigquery.SchemaField("team_abv", "STRING", mode="NULLABLE", description="Team abbreviation (e.g. 'NE', 'DAL')"),
        bigquery.SchemaField("metric", "STRING", mode="REQUIRED", description="Specific name of the metric recorded"),
        bigquery.SchemaField("category", "STRING", mode="REQUIRED", description="Group or family of related metrics"),
        bigquery.SchemaField("core_area", "STRING", mode="REQUIRED", description="High-level grouping like 'Offense', 'Defense', or 'Special Teams'"),
        bigquery.SchemaField("value", "FLOAT", mode="REQUIRED", description="Numerical value of the metric"),
    ]

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)

    try:
        client.create_table(table)
        print(f"✅ Table created: {table_ref}")
    except Exception as e:
        print(f"⚠️ Failed to create table: {e}")

if __name__ == "__main__":
    create_team_metrics_season_table()
