from google.cloud import bigquery
from src.utils.bigquery_helpers import get_bq_client
from src.config.settings import PROJECT_ID, DATASET_ID, TABLES

def create_stadiums_table():
    client = get_bq_client()

    schema = [
        bigquery.SchemaField("stadium_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("stadium_name", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("dome_flag", "BOOLEAN"),
        bigquery.SchemaField("surface_type", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLES['stadiums']}"

    table = bigquery.Table(table_id, schema=schema)
    try:
        client.get_table(table_id)
        print(f"Table {table_id} already exists.")
    except Exception:
        table = client.create_table(table)
        print(f"Created table {table_id}.")
