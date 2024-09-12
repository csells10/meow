from google.cloud import bigquery

def create_bigquery_table():
    # Define your Google Cloud project and dataset
    project_id = 'your_project_id'  # Replace with your actual project ID
    dataset_id = 'your_dataset_id'  # Replace with your actual dataset ID
    table_id = 'teams_partitioned'  # Replace with your table name

    client = bigquery.Client(project=project_id)

    # Define the schema based on the Level 1 / Level 2 structure with all necessary fields
    schema = [
        # Team Info Columns
        bigquery.SchemaField('teamID', 'STRING', mode='REQUIRED', description="Unique identifier for the team"),
        bigquery.SchemaField('teamAbv', 'STRING', mode='NULLABLE', description="Team abbreviation (e.g., MIA for Miami Dolphins)"),
        bigquery.SchemaField('teamCity', 'STRING', mode='NULLABLE', description="The city the team is from"),
        bigquery.SchemaField('teamName', 'STRING', mode='NULLABLE', description="The name of the team"),
        bigquery.SchemaField('conference', 'STRING', mode='NULLABLE', description="Conference the team belongs to"),
        bigquery.SchemaField('division', 'STRING', mode='NULLABLE', description="The team's division"),
        
        # Level 1: High-level category like 'Team Stats', 'Top Performers', 'Bye Weeks'
        bigquery.SchemaField('Level1', 'STRING', mode='REQUIRED', description="High-level category like 'Team Stats', 'Top Performers', 'Bye Weeks'"),
        
        # Level 2: The specific stat or metric under the Level 1 category
        bigquery.SchemaField('Level2', 'STRING', mode='REQUIRED', description="Specific stat or metric under the Level 1 category (e.g., 'RushingYards', 'PassingAttempts')"),
        
        # Value: The actual value for the stat (e.g., 2308 for RushingYards)
        bigquery.SchemaField('Value', 'FLOAT', mode='NULLABLE', description="The value of the stat or metric (e.g., 'RushingYards', 'PassingAttempts')"),
        
        # PlayerID: The player ID, if applicable, for top performers
        bigquery.SchemaField('PlayerID', 'STRING', mode='NULLABLE', description="Player ID for top performers (only applicable for Level1='Top Performers')"),
        
        # Date: The date the data pertains to (used for partitioning)
        bigquery.SchemaField('dataDate', 'DATE', mode='REQUIRED', description="The date the data pertains to, used for partitioning")
    ]

    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Create the table with partitioning on the dataDate column
    table = bigquery.Table(table_ref, schema=schema)

    # Partitioning based on the dataDate column
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='dataDate'  # The date the data pertains to
    )

    # Create the table
    table = client.create_table(table)  # Make an API request.
    print(f"Table {table_id} created in dataset {dataset_id}.")

# Call the function to create the table
create_bigquery_table()
