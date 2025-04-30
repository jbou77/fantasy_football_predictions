from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from src.config.settings import PROJECT_ID

def create_players_table(project_id: str, dataset_id: str):
    """Create the Players table in BigQuery if it doesn't already exist."""
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table("Players")

    schema = [
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("position", "STRING"),
        bigquery.SchemaField("team_id", "STRING"),
        bigquery.SchemaField("team_abbr", "STRING"),  # Added team abbreviation
        bigquery.SchemaField("birth_date", "DATE"),
        bigquery.SchemaField("height", "FLOAT64"),
        bigquery.SchemaField("weight", "FLOAT64"),
        bigquery.SchemaField("college", "STRING"),
        bigquery.SchemaField("draft_year", "INT64"),
        bigquery.SchemaField("draft_position", "INT64"),
        bigquery.SchemaField("active_status", "BOOLEAN"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_ref)  # Check if table exists
        print("Players table already exists.")
    except NotFound:
        # Table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print("Created Players table.")