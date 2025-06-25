from google.cloud import bigquery

def create_bigquery_table():
    project_id = 'nfl-stream-406420'
    dataset_id = 'Scores'
    table_id = 'scores_dev'

    client = bigquery.Client(project=project_id)

    schema = [
        bigquery.SchemaField('gameID', 'STRING', mode='REQUIRED', description="Unique identifier for the game"),
        bigquery.SchemaField('gameTime_epoch', 'FLOAT', mode='NULLABLE', description="Epoch timestamp of game start time in UTC"),
        bigquery.SchemaField('game_datetime_est', 'TIMESTAMP', mode='NULLABLE', description="Localized game start time in America/New_York timezone"),
        bigquery.SchemaField('game_date_est', 'DATE', mode='REQUIRED', description="Date of game in America/New_York timezone, used for partitioning"),
        bigquery.SchemaField('homePts', 'INTEGER', mode='NULLABLE', description="Total points scored by the home team"),
        bigquery.SchemaField('awayPts', 'INTEGER', mode='NULLABLE', description="Total points scored by the away team"),
        bigquery.SchemaField('teamID', 'STRING', mode='REQUIRED', description="Team identifier (one row per team, home and away)"),
        bigquery.SchemaField('Q1', 'INTEGER', mode='NULLABLE', description="Points scored in Q1 by the team"),
        bigquery.SchemaField('Q2', 'INTEGER', mode='NULLABLE', description="Points scored in Q2 by the team"),
        bigquery.SchemaField('Q3', 'INTEGER', mode='NULLABLE', description="Points scored in Q3 by the team"),
        bigquery.SchemaField('Q4', 'INTEGER', mode='NULLABLE', description="Points scored in Q4 by the team"),
    ]

    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='game_date_est'  # Partitioning on localized date
    )

    table = client.create_table(table)
    print(f"Table {table_id} created in dataset {dataset_id}.")

# Call the function to create the table
create_bigquery_table()
