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
        bigquery.SchemaField("game_id", "STRING", mode="REQUIRED", 
                             description="Unique identifier for the game (format: YYYY_WW_AWAY_HOME)"),
        bigquery.SchemaField("season_year", "INT64",
                             description="NFL season year (e.g., 2023 for the 2023-2024 season)"),
        bigquery.SchemaField("week_number", "INT64",
                             description="Week number in the NFL season (1-18 for regular season, 19+ for playoffs)"),
        bigquery.SchemaField("home_team_id", "STRING",
                             description="Team ID for the home team"),
        bigquery.SchemaField("home_team_abbr", "STRING",
                             description="Team abbreviation for the home team (e.g., KC, SF, DAL)"),
        bigquery.SchemaField("away_team_id", "STRING",
                             description="Team ID for the away team"),
        bigquery.SchemaField("away_team_abbr", "STRING",
                             description="Team abbreviation for the away team (e.g., KC, SF, DAL)"),
        bigquery.SchemaField("game_date", "DATE",
                             description="Date when the game was played (YYYY-MM-DD)"),
        bigquery.SchemaField("game_time", "STRING",
                             description="Scheduled start time of the game in Eastern Time"),
        bigquery.SchemaField("stadium_id", "STRING",
                             description="Identifier for the stadium where the game was played"),
        bigquery.SchemaField("primetime_flag", "BOOLEAN",
                             description="Indicates if the game was played in primetime (Sunday/Monday/Thursday night)"),
        bigquery.SchemaField("divisional_matchup_flag", "BOOLEAN",
                             description="Indicates if the game was between teams from the same division"),
        bigquery.SchemaField("home_score", "INT64",
                             description="Final score for the home team"),
        bigquery.SchemaField("away_score", "INT64",
                             description="Final score for the away team"),
        bigquery.SchemaField("created_at", "TIMESTAMP",
                             description="Timestamp when this record was created in the database"),
        bigquery.SchemaField("updated_at", "TIMESTAMP",
                             description="Timestamp when this record was last updated in the database"),
    ]

    try:
        client.get_table(table_ref)  # Check if table exists
        print("Games table already exists.")
    except NotFound:
        # Table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        # Add table description
        table.description = "NFL game information including teams, scores, dates, and game attributes."
        table = client.create_table(table)
        print("Created Games table.")

if __name__ == "__main__":
    create_games_table(PROJECT_ID, DATASET_ID)