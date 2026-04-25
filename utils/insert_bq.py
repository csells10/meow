from google.cloud import bigquery

def create_model_outcomes_table():
    project_id = 'nfl-stream-406420'
    dataset_id = 'Analytics'
    table_id = 'game_model_outcomes'

    client = bigquery.Client(project=project_id)

    schema = [
        bigquery.SchemaField(
            "game_id",
            "STRING",
            mode="REQUIRED",
            description="Unique game identifier in format YYYYMMDD_AWAY@HOME"
        ),
        bigquery.SchemaField(
            "season",
            "STRING",
            mode="REQUIRED",
            description="NFL season year (e.g., 2025)"
        ),
        bigquery.SchemaField(
            "game_week",
            "STRING",
            mode="REQUIRED",
            description="Week of the NFL season (e.g., 'Week 10', 'Super Bowl')"
        ),
        bigquery.SchemaField(
            "predicted_team",
            "STRING",
            mode="NULLABLE",
            description="Team the model selected as having the matchup edge"
        ),
        bigquery.SchemaField(
            "actual_winner",
            "STRING",
            mode="NULLABLE",
            description="Team that actually won the game based on final score"
        ),
        bigquery.SchemaField(
            "result",
            "STRING",
            mode="REQUIRED",
            description="Outcome of prediction: 'Correct', 'Incorrect', or 'No Pick'"
        ),
        bigquery.SchemaField(
            "confidence",
            "STRING",
            mode="NULLABLE",
            description="Model confidence level: 'High', 'Medium', or 'Low'"
        ),
        bigquery.SchemaField(
            "confidence_context",
            "STRING",
            mode="NULLABLE",
            description="Additional context affecting confidence (e.g., 'Early season — limited sample size')"
        ),
        bigquery.SchemaField(
            "created_at",
            "TIMESTAMP",
            mode="REQUIRED",
            description="Timestamp when this record was inserted into the table"
        ),
    ]

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)

    try:
        client.create_table(table)
        print(f"✅ Table created: {table_ref}")
    except Exception as e:
        print(f"⚠️ Failed to create table: {e}")

if __name__ == "__main__":
    create_model_outcomes_table()