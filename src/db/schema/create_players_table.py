from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.config.settings import DATASET_ID, PROJECT_ID

def create_players_table(project_id: str, dataset_id: str):
    """Create the Players table in BigQuery if it doesn't already exist."""
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table("Players")

    schema = [
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED",
                             description="Unique identifier for the player (GSIS ID)"),
        bigquery.SchemaField("first_name", "STRING",
                             description="Player's first name"),
        bigquery.SchemaField("last_name", "STRING",
                             description="Player's last name"),
        bigquery.SchemaField("position", "STRING",
                             description="Player's position (QB, RB, WR, TE, OL, DL, LB, DB, K, P)"),
        bigquery.SchemaField("team_id", "STRING",
                             description="Identifier of the player's current team"),
        bigquery.SchemaField("birth_date", "DATE",
                             description="Player's date of birth (YYYY-MM-DD)"),
        bigquery.SchemaField("height", "FLOAT64",
                             description="Player's height in inches"),
        bigquery.SchemaField("weight", "FLOAT64",
                             description="Player's weight in pounds"),
        bigquery.SchemaField("college", "STRING",
                             description="Name of the college/university the player attended"),
        bigquery.SchemaField("draft_year", "INT64",
                             description="Year the player was drafted into the NFL"),
        bigquery.SchemaField("draft_position", "INT64",
                             description="Overall draft position (pick number) of the player"),
        bigquery.SchemaField("active_status", "BOOLEAN",
                             description="Indicates if the player is currently active in the NFL"),
        bigquery.SchemaField("created_at", "TIMESTAMP",
                             description="Timestamp when this record was created in the database"),
        bigquery.SchemaField("updated_at", "TIMESTAMP",
                             description="Timestamp when this record was last updated in the database"),
    ]

    try:
        client.get_table(table_ref)  # Check if table exists
        print("Players table already exists.")
    except NotFound:
        # Table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        # Add table description
        table.description = "NFL player biographical and professional information including position, team, physical attributes, and draft details."
        table = client.create_table(table)

        print("Created Players table.")

if __name__ == "__main__":
    create_players_table(PROJECT_ID, DATASET_ID)
