# src/data/player_game_stats/collect_player_game_stats_data.py
import pandas as pd
import logging
import nfl_data_py as nfl
import sys
import os
from collections import defaultdict
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.utils.bigquery_helpers import get_bq_client
from src.config.settings import DATASET_ID, PROJECT_ID
from google.cloud import bigquery

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
        
        # Get player information to map IDs to positions
        player_query = f"""
        SELECT 
            player_id, 
            position 
        FROM `{PROJECT_ID}.{DATASET_ID}.Players`
        """
        client = get_bq_client()
        player_df = client.query(player_query).to_dataframe()
        player_positions = dict(zip(player_df['player_id'].astype(str), player_df['position']))
        logger.info(f"Loaded position information for {len(player_positions)} players")
        
        # Step 1: Get standard weekly stats from nfl_data_py
        logger.info("Collecting weekly player stats...")
        weekly_df = pd.DataFrame()
        
        # Process one season at a time to avoid concat issues
        for season in seasons:
            try:
                season_df = nfl.import_weekly_data(years=[season], downcast=True)
                if not season_df.empty:
                    logger.info(f"Retrieved {len(season_df)} weekly records for {season}")
                    weekly_df = pd.concat([weekly_df, season_df], ignore_index=True)
            except Exception as e:
                logger.error(f"Error retrieving weekly data for {season}: {e}")
        
        if weekly_df.empty:
            logger.error("Failed to retrieve any weekly data")
            return pd.DataFrame(), existing_game_ids
            
        logger.info(f"Successfully collected {len(weekly_df)} total weekly records")
        
        # Filter to only fantasy-relevant positions
        if 'position' in weekly_df.columns:
            fantasy_positions = ['QB', 'RB', 'WR', 'TE', 'K']
            weekly_df = weekly_df[weekly_df['position'].isin(fantasy_positions)]
            logger.info(f"Filtered to {len(weekly_df)} records for fantasy-relevant positions")
        
        # Filter to include only existing players
        weekly_df = weekly_df[weekly_df['player_id'].astype(str).isin(existing_player_ids)]
        logger.info(f"After filtering for existing players: {len(weekly_df)} records")
        
        # Now get game information for direct game_id lookup
        client = get_bq_client()
        games_query = f"""
        SELECT 
            game_id, 
            season_year, 
            week_number, 
            home_team_abbr, 
            away_team_abbr
        FROM `{PROJECT_ID}.{DATASET_ID}.Games`
        WHERE season_year IN ({','.join(str(s) for s in seasons)})
        """
        games_df = client.query(games_query).to_dataframe()
        logger.info(f"Retrieved {len(games_df)} games from database for reference")
        
        # Create a lookup dictionary for games by season, week and teams
        games_by_week = {}
        for _, game in games_df.iterrows():
            season = int(game['season_year'])
            week = int(game['week_number'])
            home = str(game['home_team_abbr'])
            away = str(game['away_team_abbr'])
            game_id = game['game_id']
            
            # Create key for season+week
            if (season, week) not in games_by_week:
                games_by_week[(season, week)] = []
            
            games_by_week[(season, week)].append({
                'game_id': game_id,
                'home_team': home,
                'away_team': away
            })
        
        # Apply game_id matching to weekly_df using reported team directly
        matched_count = 0
        for idx, row in weekly_df.iterrows():
            if pd.isna(row['season']) or pd.isna(row['week']) or pd.isna(row['player_id']):
                continue
                
            season = int(row['season'])
            week = int(row['week'])
            
            # Use recent_team from the data directly
            if 'recent_team' in weekly_df.columns and pd.notna(row['recent_team']):
                reported_team = str(row['recent_team'])
                
                # Check if we have games for this season and week
                if (season, week) in games_by_week:
                    # Look for game where the reported team was playing
                    for game in games_by_week[(season, week)]:
                        if reported_team in [game['home_team'], game['away_team']]:
                            weekly_df.at[idx, 'game_id'] = game['game_id']
                            matched_count += 1
                            break
        
        logger.info(f"Matched {matched_count} records to games using weekly data team information")
        
        # Step 2: Add kicking stats from pbp data (process one season at a time)
        logger.info("Collecting kicking stats from play-by-play data...")
        
        # Process kicking data by season to avoid the index error
        kicking_stats = []
        
        for season in seasons:
            try:
                # Define the columns we need from play-by-play data
                pbp_columns = [
                    'game_id', 'play_id', 'season', 'week', 'posteam', 'defteam', 
                    'field_goal_attempt', 'field_goal_result', 'kick_distance',
                    'extra_point_attempt', 'extra_point_result',
                    'kicker_player_id', 'kicker_player_name'
                ]
                
                # Import one season at a time
                season_pbp = nfl.import_pbp_data(years=[season], columns=pbp_columns)
                logger.info(f"Retrieved {len(season_pbp)} play-by-play records for {season}")
                
                # Filter to only kicking plays
                kicking_plays = season_pbp[
                    (season_pbp['field_goal_attempt'] == 1) | 
                    (season_pbp['extra_point_attempt'] == 1)
                ].copy()
                
                logger.info(f"Found {len(kicking_plays)} kicking plays for {season}")
                
                # Group by game and kicker to get stats per game
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
                    
            except Exception as e:
                logger.error(f"Error processing kicking data for {season}: {e}")
        
        # Add kicking stats to the dataset
        if kicking_stats:
            kicking_df = pd.DataFrame(kicking_stats)
            logger.info(f"Created kicking stats DataFrame with {len(kicking_df)} records")
            
            # Filter to include only existing players
            kicking_df = kicking_df[kicking_df['player_id'].astype(str).isin(existing_player_ids)]
            # Filter to include only existing games
            kicking_df = kicking_df[kicking_df['game_id'].astype(str).isin(existing_game_ids)]
            logger.info(f"After filtering kickers, have {len(kicking_df)} records")
            
            # Combine with weekly stats
            # First, remove any kickers from weekly_df
            if 'position' in weekly_df.columns:
                weekly_df = weekly_df[weekly_df['position'] != 'K']
                logger.info(f"Removed kickers from weekly data, now have {len(weekly_df)} records")
            
            # Now combine with kicking_df
            combined_df = pd.concat([weekly_df, kicking_df], ignore_index=True, sort=False)
            logger.info(f"Combined dataset has {len(combined_df)} records")
            weekly_df = combined_df
        
        # One final check for game_id values
        missing_game_ids = weekly_df['game_id'].isna().sum()
        if missing_game_ids > 0:
            logger.warning(f"{missing_game_ids} records still have missing game_id values")
            
            # Remove records with missing game_ids to prevent stat_id duplication issues
            weekly_df = weekly_df[weekly_df['game_id'].notna()]
            logger.info(f"Removed records with missing game_ids, now have {len(weekly_df)} records")
        
        # Final filtering - ensure we only have game_ids that exist in our database
        weekly_df = weekly_df[weekly_df['game_id'].astype(str).isin(existing_game_ids)]
        logger.info(f"After final filtering for existing games: {len(weekly_df)} records")
        
        # Save to CSV for inspection
        weekly_df.to_csv('player_game_stats_final.csv', index=False)
            
        return weekly_df, existing_game_ids
        
    except Exception as e:
        logger.error(f"Error collecting player game stats data: {e}")
        logger.exception("Full stack trace:")
        return pd.DataFrame(), set()