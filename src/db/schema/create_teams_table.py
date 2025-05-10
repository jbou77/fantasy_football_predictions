from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.config.settings import PROJECT_ID, DATASET_ID

def create_teams_table(project_id: str, dataset_id: str):
    """Create the Teams table in BigQuery if it doesn't already exist."""
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table("Teams")

    schema = [
        bigquery.SchemaField("team_id", "STRING", mode="REQUIRED",
                            description="Unique identifier for the team"),
        bigquery.SchemaField("team_name", "STRING",
                            description="Full name of the team (e.g., Kansas City Chiefs)"),
        bigquery.SchemaField("team_city", "STRING",
                            description="City where the team is based"),
        bigquery.SchemaField("team_abbreviation", "STRING",
                            description="Official team abbreviation (e.g., KC, SF, DAL)"),
        bigquery.SchemaField("conference", "STRING",
                            description="Conference the team belongs to (AFC or NFC)"),
        bigquery.SchemaField("division", "STRING",
                            description="Division within the conference (East, West, North, South)"),
        bigquery.SchemaField("stadium_id", "STRING",
                            description="ID of the team's home stadium, links to Stadiums table"),
        bigquery.SchemaField("created_at", "TIMESTAMP",
                            description="Timestamp when this record was created in the database"),
        bigquery.SchemaField("updated_at", "TIMESTAMP",
                            description="Timestamp when this record was last updated in the database"),
    ]

    try:
        client.get_table(table_ref)  # Check if table exists
        print("Teams table already exists.")
    except NotFound:
        # Table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        # Add table description
        table.description = "NFL team information including name, location, conference, division, and home stadium."
        table = client.create_table(table)
        print("Created Teams table.")

if __name__ == "__main__":
    create_teams_table(PROJECT_ID, DATASET_ID)