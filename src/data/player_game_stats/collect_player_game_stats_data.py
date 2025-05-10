import pandas as pd
import logging
import nfl_data_py as nfl
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.utils.bigquery_helpers import get_bq_client
from src.config.settings import DATASET_ID, PROJECT_ID

# Configure logger
logger = logging.getLogger(__name__)

def get_existing_ids():
    """
    Retrieve existing player_ids and game_ids from the database.
    
    Returns:
        Tuple of (set of existing player_ids, set of existing game_ids)
    """
    client = get_bq_client()
    
    # Get existing player IDs
    player_query = f"""
    SELECT player_id FROM `{PROJECT_ID}.{DATASET_ID}.Players`
    """
    player_results = client.query(player_query).result()
    existing_player_ids = {row.player_id for row in player_results}
    logger.info(f"Found {len(existing_player_ids)} existing players in database")
    
    # Get existing game IDs
    game_query = f"""
    SELECT game_id FROM `{PROJECT_ID}.{DATASET_ID}.Games`
    """
    game_results = client.query(game_query).result()
    existing_game_ids = {row.game_id for row in game_results}
    logger.info(f"Found {len(existing_game_ids)} existing games in database")
    
    return existing_player_ids, existing_game_ids

def collect_player_game_stats_data(seasons: list[int] = None) -> tuple[pd.DataFrame, set]:
    """
    Collect player game statistics from NFL data py.
    Filter to include only players and games that exist in our database.
    
    Args:
        seasons: List of seasons to collect data for. If None, will use the seasons
                in our existing Games table.
        
    Returns:
        DataFrame containing raw player game statistics
    """
    # Get existing player_ids and game_ids from database
    existing_player_ids, existing_game_ids = get_existing_ids()
    
    # If seasons weren't specified, let's get the seasons we have in our games table
    if seasons is None:
        client = get_bq_client()
        season_query = f"""
        SELECT DISTINCT season_year FROM `{PROJECT_ID}.{DATASET_ID}.Games`
        ORDER BY season_year
        """
        season_results = client.query(season_query).result()
        seasons = [row.season_year for row in season_results]
        logger.info(f"Using seasons from existing Games table: {seasons}")
    
    if not seasons:
        logger.error("No seasons to collect data for")
        return pd.DataFrame()
    
    try:
        # Use nfl_data_py to get weekly player stats
        weekly_df = nfl.import_weekly_data(years=seasons, downcast=True)
        # weekly_df.to_csv('import_weekly_data')
        initial_count = len(weekly_df)
        logger.info(f"Initially collected {initial_count} player game records")
        
        # Filter to include only existing players
        weekly_df = weekly_df[weekly_df['player_id'].astype(str).isin(existing_player_ids)]
        after_player_filter = len(weekly_df)
        logger.info(f"After filtering for existing players: {after_player_filter} records " +
                   f"({initial_count - after_player_filter} removed)")
        
        # Filter to include only existing games
        # weekly_df = weekly_df[weekly_df['game_id'].astype(str).isin(existing_game_ids)]
        # after_game_filter = len(weekly_df)
        # logger.info(f"After filtering for existing games: {after_game_filter} records " +
        #            f"({after_player_filter - after_game_filter} removed)")
        
        # weekly_df.to_csv('import_weekly_data_new')

        return weekly_df, existing_game_ids
        
    except Exception as e:
        logger.error(f"Error collecting weekly player data: {e}")
        return pd.DataFrame(), set()
    
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # You can specify specific seasons like [2020, 2021, 2022] as an argument
        # or leave it as None to get the last 5 seasons
        collect_player_game_stats_data()
    except Exception as e:
        logger.error(f"Failed to update PlayerGameStats table: {e}", exc_info=True)
        sys.exit(1)