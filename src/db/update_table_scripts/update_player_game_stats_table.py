import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.config.settings import DATASET_ID, PROJECT_ID
from src.db.truncate_tables import truncate_table, verify_table_empty
from src.data.player_game_stats.collect_player_game_stats_data import collect_player_game_stats_data
from src.data.player_game_stats.transform_player_game_stats_data import transform_player_game_stats_data
from src.data.player_game_stats.upload_player_game_stats_data import upload_player_game_stats_data

# Configure logger
logger = logging.getLogger(__name__)

def update_player_game_stats_table(seasons: list[int] = None):
    """
    Complete process to update PlayerGameStats table:
    1. Truncate the existing table
    2. Verify the table is empty
    3. Collect new data from NFL API
    4. Transform the data
    5. Upload to BigQuery
    
    Args:
        seasons: List of seasons to collect data for. If None, collects last 5 seasons.
    """
    logger.info("Starting PlayerGameStats table update process")
    
    # Step 1: Truncate the table
    logger.info("Truncating existing data from PlayerGameStats table")
    truncate_table(PROJECT_ID, DATASET_ID, "PlayerGameStats")

    # Step 2: Verify the table is empty - will raise an exception if not empty
    verify_table_empty(PROJECT_ID, DATASET_ID, "PlayerGameStats", fail_if_not_empty=True)

    # Step 3: Import player game stats data using nfl_data_py
    logger.info("Collecting fresh player game statistics from NFL API")
    weekly_df, game_ids = collect_player_game_stats_data(seasons)
    logger.info(f"Collected data for {len(weekly_df)} player game records")

    # Step 4: Transform the data from NFL data py to our schema
    logger.info("Transforming player game statistics to match database schema")
    transformed_df = transform_player_game_stats_data(weekly_df, game_ids)
    logger.info(f"Transformed {len(transformed_df)} records")
    
    # Step 5: Upload the transformed data to BigQuery
    logger.info("Uploading transformed data to BigQuery")
    upload_player_game_stats_data(transformed_df)
    
    logger.info("PlayerGameStats table update completed successfully")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # You can specify specific seasons like [2020, 2021, 2022] as an argument
        # or leave it as None to get the last 5 seasons
        update_player_game_stats_table()
    except Exception as e:
        logger.error(f"Failed to update PlayerGameStats table: {e}", exc_info=True)
        sys.exit(1)