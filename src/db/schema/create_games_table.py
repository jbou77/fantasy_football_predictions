from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.config.settings import PROJECT_ID, DATASET_ID

def create_games_table(project_id: str, dataset_id: str):
    """Create the Games table in BigQuery if it doesn't already exist."""
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table("Games")

    schema = [
        bigquery.SchemaField("game_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("season_year", "INT64"),
        bigquery.SchemaField("week_number", "INT64"),
        bigquery.SchemaField("home_team_id", "STRING"),
        bigquery.SchemaField("home_team_abbr", "STRING"),  # Added home team abbreviation to map to players table
        bigquery.SchemaField("away_team_id", "STRING"),
        bigquery.SchemaField("away_team_abbr", "STRING"),  # Added away team abbreviation to map to players table
        bigquery.SchemaField("game_date", "DATE"),
        bigquery.SchemaField("game_time", "STRING"),
        bigquery.SchemaField("stadium_id", "STRING"),
        bigquery.SchemaField("primetime_flag", "BOOLEAN"),
        bigquery.SchemaField("divisional_matchup_flag", "BOOLEAN"),
        bigquery.SchemaField("home_score", "INT64"),
        bigquery.SchemaField("away_score", "INT64"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_ref)  # Check if table exists
        print("Games table already exists.")
    except NotFound:
        # Table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print("Created Games table.")

if __name__ == "__main__":
    create_games_table(PROJECT_ID, DATASET_ID)        