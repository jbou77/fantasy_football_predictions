import nfl_data_py as nfl
import pandas as pd
import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.config.settings import DATASET_ID, PROJECT_ID
from src.db.truncate_tables import truncate_table, verify_table_empty
from src.data.players.transform_player_data import transform_player_data
from src.data.players.upload_player_data_to_bq import upload_basic_player_data
from src.data.players.collect_basic_player_data import collect_basic_player_data

# Configure logger
logger = logging.getLogger(__name__)

def update_players_table():
    """
    Complete process to update Players table:
    1. Truncate the existing table
    2. Collect new data from NFL API
    3. Transform the data
    4. Upload to BigQuery
    """
    logger.info("Starting Players table update process")
    
    # Step 1: Truncate the table
    logger.info("Truncating existing data from Players table")
    truncate_table(PROJECT_ID, DATASET_ID, "Players")

    # Step 2: Verify the table is empty - will raise an exception if not empty
    verify_table_empty(PROJECT_ID, DATASET_ID, "Players", fail_if_not_empty=True)

    # Step 3: Import player data using nfl_data_py
    logger.info("Collecting fresh player data from NFL API")
    players_df = collect_basic_player_data()
    logger.info(f"Collected data for {len(players_df)} active players")

    # Step 4: Transform the data from NFL data py to our schema
    logger.info("Transforming player data to match database schema")
    transformed_df = transform_player_data(players_df)
    
    # Step 5: Upload the transformed data to BigQuery
    logger.info("Uploading transformed data to BigQuery")
    upload_basic_player_data(transformed_df)
    
    logger.info("Players table update completed successfully")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        update_players_table()
    except Exception as e:
        logger.error(f"Failed to update Players table: {e}", exc_info=True)
        sys.exit(1)