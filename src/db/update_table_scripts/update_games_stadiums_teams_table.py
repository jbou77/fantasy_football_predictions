import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.config.settings import DATASET_ID, PROJECT_ID
from src.db.truncate_tables import truncate_table, verify_table_empty
from src.data.games_stadiums_teams.collect_games_stadiums_teams_data import collect_games_stadiums_teams_data
from src.data.games_stadiums_teams.transform_games_data import transform_games_data
from src.data.games_stadiums_teams.transform_stadiums_data import transform_stadiums_data
from src.data.games_stadiums_teams.transform_teams_data import transform_teams_data
from src.data.games_stadiums_teams.upload_games_stadiums_teams_data_to_bq import upload_games_data, upload_stadiums_data, upload_teams_data

# Configure logger
logger = logging.getLogger(__name__)

def update_games_stadiums_teams_tables(seasons: list[int] = None):
    """
    Complete process to update Games, Stadiums, and Teams tables:
    1. Collect data once from NFL API
    2. Update Games table
    3. Update Stadiums table
    4. Update Teams table
    
    Args:
        seasons: List of seasons to collect data for. If None, collects last 5 seasons.
    """
    logger.info("Starting update process for Games, Stadiums, and Teams tables")
    
    # Step 1: Collect data once for all tables
    logger.info("Collecting NFL data from API")
    schedules_df = collect_games_stadiums_teams_data(seasons)
    logger.info(f"Collected data for {len(schedules_df)} games")
    
    # # Step 2: Update Games table
    logger.info("Updating Games table...")
    truncate_table(PROJECT_ID, DATASET_ID, "Games")
    verify_table_empty(PROJECT_ID, DATASET_ID, "Games", fail_if_not_empty=True)
    
    transformed_games_df = transform_games_data(schedules_df)
    upload_games_data(transformed_games_df)
    logger.info("Games table update completed")
    
    # Step 3: Update Stadiums table
    logger.info("Updating Stadiums table...")
    truncate_table(PROJECT_ID, DATASET_ID, "Stadiums")
    verify_table_empty(PROJECT_ID, DATASET_ID, "Stadiums", fail_if_not_empty=True)
    
    transformed_stadiums_df = transform_stadiums_data(schedules_df)
    upload_stadiums_data(transformed_stadiums_df)
    logger.info("Stadiums table update completed")
    
    # Step 4: Update Teams table
    logger.info("Updating Teams table...")
    truncate_table(PROJECT_ID, DATASET_ID, "Teams")
    verify_table_empty(PROJECT_ID, DATASET_ID, "Teams", fail_if_not_empty=True)
    
    transformed_teams_df = transform_teams_data(schedules_df)
    upload_teams_data(transformed_teams_df)
    logger.info("Teams table update completed")
    
    logger.info("All tables have been successfully updated")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # You can specify specific seasons like [2020, 2021, 2022] as an argument
        # or leave it as None to get the last 5 seasons
        update_games_stadiums_teams_tables()
    except Exception as e:
        logger.error(f"Failed to update tables: {e}", exc_info=True)
        sys.exit(1)