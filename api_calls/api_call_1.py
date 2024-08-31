import requests
from google.cloud import bigquery
from utils.helper import process_data

def fetch_data():
    response = requests.get('https://api.example.com/data_1')
    if response.status_code == 200:
        data = response.json()
        processed_data = process_data(data)
        insert_into_bigquery(processed_data)

def insert_into_bigquery(data):
    client = bigquery.Client()
    table_id = "your-project.your_dataset.your_table_1"
    errors = client.insert_rows_json(table_id, [data])
    if errors == []:
        print("Data successfully inserted.")
    else:
        print("Errors occurred: ", errors)
