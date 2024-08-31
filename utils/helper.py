from google.cloud import bigquery

def insert_into_bigquery(table_id, rows_to_insert):
    """
    Inserts rows into a specified BigQuery table.

    :param table_id: Full table ID in the format 'project_id.dataset_id.table_id'
    :param rows_to_insert: List of dictionaries, where each dictionary represents a row to insert.
    :return: None
    """
    client = bigquery.Client()
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print(f"Encountered errors while inserting rows: {errors}")
