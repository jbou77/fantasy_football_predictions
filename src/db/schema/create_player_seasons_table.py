from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import sys
import os
import nfl_data_py as nfl
import pandas as pd
from datetime import datetime
from pandas import Timestamp
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.config.settings import DATASET_ID, PROJECT_ID

def create_player_seasons_table(project_id: str, dataset_id: str):
    """Create the PlayerSeasons table in BigQuery if it doesn't already exist."""
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table("PlayerSeasons")

    schema = [
        bigquery.SchemaField("player_season_id", "STRING", mode="REQUIRED",
                             description="Unique identifier for the player-season record (player_id + season_year)"),
        bigquery.SchemaField("player_id", "STRING", mode="REQUIRED",
                             description="Player identifier, foreign key to Players table"),
        bigquery.SchemaField("season_year", "INT64", mode="REQUIRED",
                             description="NFL season year"),
        bigquery.SchemaField("team_id", "STRING",
                             description="Identifier of the player's team for this season"),
        bigquery.SchemaField("team_abbr", "STRING",
                             description="Abbreviation of the player's team for this season"),
        bigquery.SchemaField("primary_position", "STRING",
                             description="Player's primary position during this season"),
        bigquery.SchemaField("created_at", "TIMESTAMP",
                             description="Timestamp when this record was created in the database"),
        bigquery.SchemaField("updated_at", "TIMESTAMP",
                             description="Timestamp when this record was last updated in the database"),
    ]

    try:
        client.get_table(table_ref)  # Check if table exists
        print("PlayerSeasons table already exists.")
    except NotFound:
        # Table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        # Add table description
        table.description = "NFL player team and position information per season."
        table = client.create_table(table)
        print("Created PlayerSeasons table.")
        
        # Get the seasons we have in our Games table
        seasons_query = f"""
        SELECT DISTINCT season_year 
        FROM `{project_id}.{dataset_id}.Games`
        ORDER BY season_year
        """
        seasons_df = client.query(seasons_query).to_dataframe()
        seasons = [int(row['season_year']) for _, row in seasons_df.iterrows()]
        
        if not seasons:
            print("No seasons found in Games table. Cannot populate PlayerSeasons.")
            return
            
        print(f"Fetching roster data for seasons: {seasons}")
        
        # Get existing player IDs to filter the roster data
        players_query = f"""
        SELECT player_id FROM `{project_id}.{dataset_id}.Players`
        """
        players_df = client.query(players_query).to_dataframe()
        existing_player_ids = set(players_df['player_id'].astype(str))
        
        # Fetch player weekly data from nfl_data_py for each season - this has team info
        all_seasons_data = []
        for season in seasons:
            try:
                # Get weekly data which includes player team for that season
                weekly_df = nfl.import_weekly_data([season], downcast=True)
                print(f"Fetched {len(weekly_df)} weekly records for {season}")
                
                if not weekly_df.empty:
                    # Group by player_id, season, team to get most common position
                    season_summary = (
                        weekly_df.groupby(['player_id', 'recent_team'])
                        .agg({
                            'position': lambda x: x.mode().iloc[0] if not x.mode().empty else None
                        })
                        .reset_index()
                    )
                    season_summary['season_year'] = season
                    all_seasons_data.append(season_summary)
            except Exception as e:
                print(f"Error fetching weekly data for {season}: {e}")
        
        if not all_seasons_data:
            print("Could not fetch any seasonal data. Cannot populate PlayerSeasons.")
            return
            
        # Combine all season data
        combined_df = pd.concat(all_seasons_data, ignore_index=True)
        
        # Filter to include only players in our database
        combined_df = combined_df[combined_df['player_id'].astype(str).isin(existing_player_ids)]
        print(f"After filtering, have {len(combined_df)} player-season records for players in our database")
        
        # Create current timestamp as a pandas Timestamp
        now = Timestamp(datetime.now())
        
        # Prepare data for BigQuery
        player_seasons = []
        for _, row in combined_df.iterrows():
            player_id = str(row['player_id'])
            season = int(row['season_year'])
            team_abbr = str(row['recent_team'])
            position = row['position']
            
            player_seasons.append({
                'player_season_id': f"{player_id}_{season}",
                'player_id': player_id,
                'season_year': season,
                'team_id': team_abbr,  # Using team abbreviation as ID for simplicity
                'team_abbr': team_abbr,
                'primary_position': position,
                'created_at': now,  # Use pandas Timestamp object
                'updated_at': now   # Use pandas Timestamp object
            })
        
        if not player_seasons:
            print("No valid player seasons data could be created.")
            return
            
        # Remove duplicates based on player_season_id
        unique_seasons = {}
        for ps in player_seasons:
            if ps['player_season_id'] not in unique_seasons:
                unique_seasons[ps['player_season_id']] = ps
        
        player_seasons = list(unique_seasons.values())
        print(f"After removing duplicates, have {len(player_seasons)} unique player-season records")
            
        # Convert to DataFrame
        player_seasons_df = pd.DataFrame(player_seasons)
        
        # Load to BigQuery
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("player_season_id", "STRING"),
                bigquery.SchemaField("player_id", "STRING"),
                bigquery.SchemaField("season_year", "INTEGER"),
                bigquery.SchemaField("team_id", "STRING"),
                bigquery.SchemaField("team_abbr", "STRING"),
                bigquery.SchemaField("primary_position", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP"),
                bigquery.SchemaField("updated_at", "TIMESTAMP"),
            ],
            write_disposition="WRITE_APPEND",
        )
        
        # Install pandas-gbq for better BigQuery support if needed
        try:
            import pip
            pip.main(['install', 'pandas-gbq'])
            print("Installed pandas-gbq for better BigQuery support")
        except Exception as e:
            print(f"Could not install pandas-gbq: {e}")
        
        try:
            job = client.load_table_from_dataframe(
                player_seasons_df, 
                f"{project_id}.{dataset_id}.PlayerSeasons", 
                job_config=job_config
            )
            job.result()  # Wait for the job to complete
            print(f"Loaded {len(player_seasons_df)} player-season records to BigQuery")
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            print("Trying alternative loading method...")
            
            # Alternative loading method - save to CSV and load from there
            try:
                temp_csv = "player_seasons_temp.csv"
                player_seasons_df.to_csv(temp_csv, index=False)
                
                # Create job config for loading from CSV
                csv_job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    skip_leading_rows=1,
                    source_format=bigquery.SourceFormat.CSV,
                    write_disposition="WRITE_APPEND",
                )
                
                with open(temp_csv, "rb") as source_file:
                    job = client.load_table_from_file(
                        source_file,
                        f"{project_id}.{dataset_id}.PlayerSeasons",
                        job_config=csv_job_config
                    )
                
                job.result()  # Wait for the job to complete
                print(f"Loaded {len(player_seasons_df)} player-season records to BigQuery using CSV method")
                
                # Clean up temp file
                os.remove(temp_csv)
            except Exception as csv_error:
                print(f"Error with alternative loading method: {csv_error}")

if __name__ == "__main__":
    create_player_seasons_table(PROJECT_ID, DATASET_ID)