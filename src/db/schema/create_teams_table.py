from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from src.config.settings import PROJECT_ID

def create_teams_table(project_id: str, dataset_id: str):
    """Create the Teams table in BigQuery if it doesn't already exist."""
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table("Teams")

    schema = [
        bigquery.SchemaField("team_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("team_name", "STRING"),
        bigquery.SchemaField("team_city", "STRING"),
        bigquery.SchemaField("team_abbreviation", "STRING"),
        bigquery.SchemaField("conference", "STRING"),
        bigquery.SchemaField("division", "STRING"),
        bigquery.SchemaField("stadium_id", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_ref)  # Check if table exists
        print("Teams table already exists.")
    except NotFound:
        # Table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print("Created Teams table.")

