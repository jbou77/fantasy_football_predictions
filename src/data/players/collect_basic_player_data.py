import nfl_data_py as nfl
import pandas as pd
from datetime import datetime
import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.utils.bigquery_helpers import get_bq_client
from src.config.settings import DATASET_ID, PROJECT_ID
import numpy as np
from src.db.truncate_tables import truncate_table, verify_table_empty
from src.data.players.transform_player_data import transform_player_data

# Configure logger
logger = logging.getLogger(__name__)

def collect_basic_player_data():
    """Collect basic player data from NFL data source and upload to BigQuery."""
    # First truncate the table
    truncate_table(PROJECT_ID, DATASET_ID, "Players")

    # Verify the table is empty - will raise an exception if not empty
    verify_table_empty(PROJECT_ID, DATASET_ID, "Players", fail_if_not_empty=True)

    # Import player data using nfl_data_py
    players_df = nfl.import_players()
    logger.debug(f"Columns in raw data: {players_df.columns}")
    
    # Filter for active players (consider filtering to only offensive players)
    players_df = players_df[players_df['status_short_description'] == 'Active']

    # Transform the data from nfl data py to our schema
    logger.info("Transforming player data...")
    players_df = transform_player_data(players_df)
    
    logger.info(f"Transformed data contains {len(players_df)} records")
    
    # Now upload the data to BigQuery
    upload_basic_player_data(players_df)

def upload_basic_player_data(players_df: pd.DataFrame) -> None:
    """
    Upload player data to BigQuery.
    
    Args:
        players_df: DataFrame containing processed player data
    """
    logger.info("Starting upload to BigQuery...")
    
    # Initialize BigQuery client
    client = get_bq_client()

    # Define BigQuery dataset and table name
    table_id = f"{PROJECT_ID}.{DATASET_ID}.Players"

    # Prepare the data for BigQuery upload - ensure NaN values are replaced with None
    rows_to_insert = players_df.replace({np.nan: None}).to_dict(orient="records")
    logger.info(f"Prepared {len(rows_to_insert)} rows for insertion")

    # Insert the rows into BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if errors == []:
        logger.info(f"Successfully uploaded {len(players_df)} players to BigQuery.")
    else:
        logger.error(f"Error uploading players: {errors}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    collect_basic_player_data()