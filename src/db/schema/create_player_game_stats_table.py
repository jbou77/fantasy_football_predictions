from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.config.settings import DATASET_ID, PROJECT_ID

def create_player_game_stats_table(project_id: str, dataset_id: str):
    """Create the PlayerGameStats table in BigQuery if it doesn't already exist."""
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table("PlayerGameStats")

    schema = [
        bigquery.SchemaField("stat_id", "STRING", mode="REQUIRED",
                             description="Unique identifier for the stat record (composite of player_id and game_id)"),
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED",
                             description="Player identifier, foreign key to Players table"),
        bigquery.SchemaField("game_id", "STRING", mode="REQUIRED",
                             description="Game identifier, foreign key to Games table"),
        bigquery.SchemaField("team_id", "STRING",
                             description="Team identifier, foreign key to Teams table"),
        bigquery.SchemaField("position_played", "STRING",
                             description="Position played during this game"),
        bigquery.SchemaField("snaps_played", "INT64",
                             description="Number of snaps played in the game"),
        bigquery.SchemaField("starter_flag", "BOOLEAN",
                             description="Indicates if player started the game"),
        
        # Passing stats
        bigquery.SchemaField("passing_attempts", "INT64",
                             description="Number of passing attempts"),
        bigquery.SchemaField("passing_completions", "INT64",
                             description="Number of completed passes"),
        bigquery.SchemaField("passing_yards", "INT64",
                             description="Total passing yards"),
        bigquery.SchemaField("passing_tds", "INT64",
                             description="Number of passing touchdowns"),
        bigquery.SchemaField("passing_ints", "INT64",
                             description="Number of interceptions thrown"),
        
        # Rushing stats
        bigquery.SchemaField("rushing_attempts", "INT64",
                             description="Number of rushing attempts"),
        bigquery.SchemaField("rushing_yards", "INT64",
                             description="Total rushing yards"),
        bigquery.SchemaField("rushing_tds", "INT64",
                             description="Number of rushing touchdowns"),
        
        # Receiving stats
        bigquery.SchemaField("receiving_targets", "INT64",
                             description="Number of times targeted for a pass"),
        bigquery.SchemaField("receptions", "INT64",
                             description="Number of passes caught"),
        bigquery.SchemaField("receiving_yards", "INT64",
                             description="Total receiving yards"),
        bigquery.SchemaField("receiving_tds", "INT64",
                             description="Number of receiving touchdowns"),
        
        # Fumble stats
        bigquery.SchemaField("fumbles", "INT64",
                             description="Total fumbles"),
        bigquery.SchemaField("fumbles_lost", "INT64",
                             description="Number of fumbles lost to the opposing team"),
        
        # Kicking stats
        bigquery.SchemaField("field_goals_attempted", "INT64",
                             description="Number of field goals attempted"),
        bigquery.SchemaField("field_goals_made", "INT64",
                             description="Number of field goals made"),
        bigquery.SchemaField("extra_points_attempted", "INT64",
                             description="Number of extra points attempted"),
        bigquery.SchemaField("extra_points_made", "INT64",
                             description="Number of extra points made"),
        
        # Defensive stats
        bigquery.SchemaField("defensive_sacks", "FLOAT64",
                             description="Number of sacks (can be partial)"),
        bigquery.SchemaField("defensive_tackles", "INT64",
                             description="Total tackles (solo + assisted)"),
        bigquery.SchemaField("defensive_interceptions", "INT64",
                             description="Number of interceptions caught on defense"),
        bigquery.SchemaField("defensive_fumbles_recovered", "INT64",
                             description="Number of fumbles recovered on defense"),
        bigquery.SchemaField("defensive_tds", "INT64",
                             description="Number of defensive touchdowns scored"),
        
        # Return stats
        bigquery.SchemaField("punt_returns", "INT64",
                             description="Number of punt returns"),
        bigquery.SchemaField("punt_return_yards", "INT64",
                             description="Total punt return yards"),
        bigquery.SchemaField("punt_return_tds", "INT64",
                             description="Number of punt return touchdowns"),
        bigquery.SchemaField("kick_returns", "INT64",
                             description="Number of kickoff returns"),
        bigquery.SchemaField("kick_return_yards", "INT64",
                             description="Total kickoff return yards"),
        bigquery.SchemaField("kick_return_tds", "INT64",
                             description="Number of kickoff return touchdowns"),
        
        # Metadata
        bigquery.SchemaField("created_at", "TIMESTAMP",
                             description="Timestamp when this record was created in the database"),
        bigquery.SchemaField("updated_at", "TIMESTAMP",
                             description="Timestamp when this record was last updated in the database"),
    ]

    try:
        client.get_table(table_ref)  # Check if table exists
        print("PlayerGameStats table already exists.")
    except NotFound:
        # Table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        # Add table description
        table.description = "NFL player game statistics including passing, rushing, receiving, kicking, defensive, and return stats."
        table = client.create_table(table)
        print("Created PlayerGameStats table.")

if __name__ == "__main__":
    create_player_game_stats_table(PROJECT_ID, DATASET_ID)