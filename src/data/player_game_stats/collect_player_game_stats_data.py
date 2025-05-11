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
    Collect player game statistics from NFL data py, including kicking stats from play-by-play data.
    Filter to include only players and games that exist in our database.
    
    Args:
        seasons: List of seasons to collect data for. If None, will use the seasons
                in our existing Games table.
        
    Returns:
        Tuple of (DataFrame containing raw player game statistics, set of existing game IDs)
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
        return pd.DataFrame(), set()
    
    try:
        # Convert seasons to integers if they aren't already
        seasons = [int(s) for s in seasons]
        
        # Step 1: Use nfl_data_py to get weekly player stats (usual stats)
        logger.info("Collecting weekly player stats...")
        weekly_df = nfl.import_weekly_data(years=seasons, downcast=True)
        initial_count = len(weekly_df)
        # weekly_df.to_csv('import_weekly_data.csv', index=False)
        logger.info(f"Initially collected {initial_count} player game records")
        
        # Generate game_id for all player stats in weekly_df 
        logger.info("Generating game_ids for all player stats...")
        if all(col in weekly_df.columns for col in ['season', 'week', 'recent_team', 'opponent_team']):
            # Get info about games from our database to determine home/away teams
            client = get_bq_client()
            games_query = f"""
            SELECT game_id, season_year, week_number, home_team_abbr, away_team_abbr 
            FROM `{PROJECT_ID}.{DATASET_ID}.Games`
            WHERE season_year IN ({','.join(str(s) for s in seasons)})
            """
            games_df = client.query(games_query).to_dataframe()
            logger.info(f"Retrieved {len(games_df)} games from database for reference")
            
            # Create a lookup dictionary for games
            games_lookup = {}
            for _, game in games_df.iterrows():
                season = str(game['season_year'])
                week = str(game['week_number']).zfill(2)
                home = game['home_team_abbr']
                away = game['away_team_abbr']
                
                # Create keys for team+opponent combinations
                home_key = (season, week, home, away)
                away_key = (season, week, away, home)
                
                # Create lookup for both possible combinations
                games_lookup[home_key] = game['game_id']
                games_lookup[away_key] = game['game_id']
            
            # Apply lookup to weekly_df
            matched_count = 0
            for idx, row in weekly_df.iterrows():
                if pd.notna(row['season']) and pd.notna(row['week']) and pd.notna(row['recent_team']) and pd.notna(row['opponent_team']):
                    season = str(row['season'])
                    week = str(row['week']).zfill(2)
                    team = str(row['recent_team'])
                    opponent = str(row['opponent_team'])
                    
                    # Try both team orderings to find a match
                    lookup_key = (season, week, team, opponent)
                    reverse_key = (season, week, opponent, team)
                    
                    if lookup_key in games_lookup:
                        weekly_df.at[idx, 'game_id'] = games_lookup[lookup_key]
                        matched_count += 1
                    elif reverse_key in games_lookup:
                        weekly_df.at[idx, 'game_id'] = games_lookup[reverse_key]
                        matched_count += 1
            
            logger.info(f"Matched {matched_count} records to existing game_ids")
            
            # For rows without game_id, try to construct one based on the pattern
            # For these we'll need to guess home/away based on our best knowledge
            unmatched = weekly_df[weekly_df['game_id'].isna()]
            logger.info(f"Attempting to construct game_ids for {len(unmatched)} unmatched records")
            
            for idx, row in unmatched.iterrows():
                if pd.notna(row['season']) and pd.notna(row['week']) and pd.notna(row['recent_team']) and pd.notna(row['opponent_team']):
                    season = str(row['season'])
                    week = str(row['week']).zfill(2)
                    team = str(row['recent_team'])
                    opponent = str(row['opponent_team'])
                    
                    # Try both possible orderings and check if they match existing game_ids
                    possible_id1 = f"{season}_{week}_{team}_{opponent}"
                    possible_id2 = f"{season}_{week}_{opponent}_{team}"
                    
                    if possible_id1 in existing_game_ids:
                        weekly_df.at[idx, 'game_id'] = possible_id1
                    elif possible_id2 in existing_game_ids:
                        weekly_df.at[idx, 'game_id'] = possible_id2
            
            # Final count of records with game_ids
            with_game_id = weekly_df['game_id'].notna().sum()
            logger.info(f"After all matching: {with_game_id} of {len(weekly_df)} records have game_ids")
        else:
            logger.warning("Missing necessary columns (season, week, recent_team, opponent_team) to generate game_ids")
        
        # Step 2: Collect kicking data from play-by-play data
        logger.info("Collecting kicking stats from play-by-play data...")
        
        # Define the columns we need from play-by-play data
        pbp_columns = [
            'game_id', 'play_id', 'season', 'week', 'posteam', 'defteam', 
            'field_goal_attempt', 'field_goal_result', 'kick_distance',
            'extra_point_attempt', 'extra_point_result',
            'kicker_player_id', 'kicker_player_name',
            'touchdown', 'punt_blocked'
        ]
        
        # Import play-by-play data with just the columns we need
        try:
            pbp_df = nfl.import_pbp_data(years=seasons, columns=pbp_columns, downcast=True)
            logger.info(f"Collected {len(pbp_df)} play-by-play records for kicking analysis")
            
            # Filter to only kicking plays
            kicking_plays = pbp_df[
                (pbp_df['field_goal_attempt'] == 1) | 
                (pbp_df['extra_point_attempt'] == 1)
            ].copy()
            
            # Get unique kicking stats per player per game
            if len(kicking_plays) > 0:
                logger.info(f"Found {len(kicking_plays)} kicking plays")
                
                # Group by game and kicker to get stats per game
                kicking_stats = []
                
                for (game_id, kicker_id), group in kicking_plays.groupby(['game_id', 'kicker_player_id']):
                    if pd.isna(kicker_id) or pd.isna(game_id):
                        continue
                        
                    # Get a sample row for player info
                    sample_row = group.iloc[0]
                    
                    # Calculate kicking stats
                    fg_attempts = sum(group['field_goal_attempt'] == 1)
                    fg_made = sum((group['field_goal_attempt'] == 1) & (group['field_goal_result'] == 'made'))
                    
                    pat_attempts = sum(group['extra_point_attempt'] == 1)
                    pat_made = sum((group['extra_point_attempt'] == 1) & (group['extra_point_result'] == 'good'))
                    
                    # Create a record with the player's kicking stats for this game
                    kicking_stats.append({
                        'player_id': str(kicker_id),
                        'player_name': sample_row['kicker_player_name'],
                        'game_id': str(game_id),
                        'season': sample_row['season'],
                        'week': sample_row['week'],
                        'recent_team': sample_row['posteam'],  # Team with possession is the kicker's team
                        'position': 'K',  # Assume all kickers have position 'K'
                        'fg_attempts': fg_attempts,
                        'fg_made': fg_made,
                        'pat_attempts': pat_attempts,
                        'pat_made': pat_made
                    })
                
                # Convert to DataFrame
                if kicking_stats:
                    kicking_df = pd.DataFrame(kicking_stats)
                    logger.info(f"Created kicking stats DataFrame with {len(kicking_df)} records")
                    
                    # Combine with weekly stats
                    logger.info("Combining weekly stats with kicking stats...")
                    
                    # We need to ensure no duplicates if a kicker already appears in weekly_df
                    # First, find existing kicker records in weekly_df
                    weekly_kickers = weekly_df[
                        (weekly_df['position'] == 'K') & 
                        weekly_df['player_id'].isin(kicking_df['player_id'])
                    ].copy()
                    
                    # Remove those records from weekly_df
                    if not weekly_kickers.empty:
                        logger.info(f"Found {len(weekly_kickers)} kicker records in weekly data that will be replaced")
                        weekly_df = weekly_df[~(
                            (weekly_df['position'] == 'K') & 
                            weekly_df['player_id'].isin(kicking_df['player_id'])
                        )]
                    
                    # Now combine with kicking_df
                    combined_df = pd.concat([weekly_df, kicking_df], ignore_index=True, sort=False)
                    logger.info(f"Combined dataset has {len(combined_df)} records")
                    weekly_df = combined_df
            else:
                logger.warning("No kicking plays found in play-by-play data")
                
        except Exception as e:
            logger.error(f"Error collecting play-by-play kicking data: {e}")
            logger.warning("Proceeding with regular weekly stats only")
        
        # Filter to include only existing players
        weekly_df = weekly_df[weekly_df['player_id'].astype(str).isin(existing_player_ids)]
        after_player_filter = len(weekly_df)
        logger.info(f"After filtering for existing players: {after_player_filter} records " +
                   f"({initial_count - after_player_filter} removed)")
        
        # One final check for game_id values
        missing_game_ids = weekly_df['game_id'].isna().sum()
        if missing_game_ids > 0:
            logger.warning(f"{missing_game_ids} records still have missing game_id values")
            
        # weekly_df.to_csv('weekly_player_stats_with_game_ids.csv', index=False)
        return weekly_df, existing_game_ids
        
    except Exception as e:
        logger.error(f"Error collecting player game stats data: {e}")
        logger.exception("Full stack trace:")
        return pd.DataFrame(), set()
    
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Test with recent seasons
        collect_player_game_stats_data()
    except Exception as e:
        logger.error(f"Test run failed: {e}", exc_info=True)
        sys.exit(1)