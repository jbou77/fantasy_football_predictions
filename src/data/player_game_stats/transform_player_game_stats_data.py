# src/data/player_game_stats/transform_player_game_stats_data.py
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from src.utils.bigquery_helpers import get_bq_client
from google.cloud import bigquery
from src.config.settings import DATASET_ID, PROJECT_ID

# Configure logger
logger = logging.getLogger(__name__)

def transform_player_game_stats_data(weekly_df: pd.DataFrame, game_ids: set) -> pd.DataFrame:
    """
    Transform raw player game stats into the format required for database storage.
    
    Args:
        weekly_df: Raw player game stats DataFrame from NFL data source
        game_ids: Set of existing game IDs in format "YYYY_WW_AWAY_HOME"
        
    Returns:
        Transformed DataFrame ready for BigQuery upload
    """
    logger.debug("Starting transformation of player game stats data")
    
    # Check if we have a valid dataframe to transform
    if weekly_df.empty:
        logger.warning("Received empty DataFrame for transformation, returning empty result")
        return pd.DataFrame()
    
    # Check required columns
    required_columns = ['player_id', 'game_id']
    missing_columns = [col for col in required_columns if col not in weekly_df.columns]
    if missing_columns:
        logger.error(f"Missing required columns in input data: {missing_columns}")
        return pd.DataFrame()
    
    # Create a copy of the dataframe to avoid modifying the original
    df = weekly_df.copy()
    
    # Drop any rows with missing player_id or game_id
    original_count = len(df)
    df = df.dropna(subset=['player_id', 'game_id'])
    if len(df) < original_count:
        logger.warning(f"Dropped {original_count - len(df)} rows with missing player_id or game_id")
    
    # Add timestamps
    current_time = datetime.now()
    df['created_at'] = current_time
    df['updated_at'] = current_time
    
    # Process player_id - ensure it's a string
    df['player_id'] = df['player_id'].astype(str)
    
    # Process game_id - ensure it's a string
    df['game_id'] = df['game_id'].astype(str)
    
    # Validate game_ids against PlayerSeasons data
    logger.info("Validating game_ids against player team assignments...")
    
    try:
        # Get data from PlayerSeasons table
        client = get_bq_client()
        player_seasons_query = f"""
        SELECT 
            player_id, 
            season_year, 
            team_abbr
        FROM `{PROJECT_ID}.{DATASET_ID}.PlayerSeasons`
        """
        player_seasons_df = client.query(player_seasons_query).to_dataframe()
        logger.info(f"Retrieved {len(player_seasons_df)} player-season records for validation")
        
        # Create lookup dictionary for player team by season
        player_team_by_season = {}
        for _, row in player_seasons_df.iterrows():
            player_id = str(row['player_id'])
            season = int(row['season_year'])
            team = str(row['team_abbr'])
            
            if player_id not in player_team_by_season:
                player_team_by_season[player_id] = {}
            player_team_by_season[player_id][season] = team
        
        # Get game details
        games_query = f"""
        SELECT 
            game_id, 
            season_year, 
            week_number,
            home_team_abbr, 
            away_team_abbr
        FROM `{PROJECT_ID}.{DATASET_ID}.Games`
        """
        games_df = client.query(games_query).to_dataframe()
        logger.info(f"Retrieved {len(games_df)} games for validation")
        
        # Create game lookup
        game_details = {}
        games_by_week = {}
        
        for _, game in games_df.iterrows():
            game_id = str(game['game_id'])
            season = int(game['season_year'])
            week = int(game['week_number'])
            home = str(game['home_team_abbr'])
            away = str(game['away_team_abbr'])
            
            # Store game details
            game_details[game_id] = {
                'season': season,
                'week': week,
                'teams': [home, away]
            }
            
            # Create lookup by season and week
            if (season, week) not in games_by_week:
                games_by_week[(season, week)] = []
            
            games_by_week[(season, week)].append({
                'game_id': game_id,
                'home_team': home,
                'away_team': away
            })
        
        # Validate and correct game_ids
        invalid_count = 0
        corrected_count = 0
        
        for idx, row in df.iterrows():
            player_id = str(row['player_id'])
            game_id = str(row['game_id'])
            
            # Skip if we don't have the game info
            if game_id not in game_details:
                continue
                
            season = game_details[game_id]['season']
            week = game_details[game_id]['week']
            teams_in_game = game_details[game_id]['teams']
            
            # Check if we know what team this player was on in this season
            if player_id in player_team_by_season and season in player_team_by_season[player_id]:
                player_team = player_team_by_season[player_id][season]
                
                # If player's team wasn't in this game, this is an invalid match
                if player_team not in teams_in_game:
                    logger.warning(f"Invalid game match: Player {player_id} (team: {player_team}) " +
                                  f"matched to game {game_id} with teams {teams_in_game}")
                    invalid_count += 1
                    
                    # Try to find the correct game
                    if (season, week) in games_by_week:
                        for game in games_by_week[(season, week)]:
                            if player_team in [game['home_team'], game['away_team']]:
                                # Found the correct game!
                                df.at[idx, 'game_id'] = game['game_id']
                                corrected_count += 1
                                break
                        else:
                            # No valid game found, mark as invalid
                            df.at[idx, 'game_id'] = None
        
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} records with incorrect game_ids")
            logger.info(f"Successfully corrected {corrected_count} records")
            
            # Remove records with invalid game_ids
            original_count = len(df)
            df = df[df['game_id'].notna()]
            logger.info(f"Removed {original_count - len(df)} records with invalid game_ids that couldn't be corrected")
    
    except Exception as e:
        logger.error(f"Error validating game_ids: {e}")
        logger.warning("Proceeding without game_id validation")
    
    # Create a unique stat_id for each record (player_id + game_id combination)
    df['stat_id'] = df['player_id'].astype(str) + '_' + df['game_id'].astype(str)
    
    # Save intermediate file to inspect
    df.to_csv('transform_before_dedup.csv', index=False)
    
    # Process team_id - use recent_team as team_id
    df['team_id'] = df['recent_team'].astype(str)
    
    # Process position
    df['position_played'] = df['position'].astype(str)
    
    # Handle snaps data - they might not be in the input data
    # If snaps data is missing, we'll leave it as None for now
    if 'offense_snaps' in df.columns and 'defense_snaps' in df.columns:
        # Convert to numeric first
        offense_snaps = pd.to_numeric(df['offense_snaps'], errors='coerce').fillna(0)
        defense_snaps = pd.to_numeric(df['defense_snaps'], errors='coerce').fillna(0)
        # Add them together
        df['snaps_played'] = (offense_snaps + defense_snaps).astype('Int64')
    else:
        df['snaps_played'] = None
    
    # Process starter flag - using stats to infer starter status
    # For QBs, consider starter if they had at least 10 passing attempts
    attempts = pd.to_numeric(df.get('attempts', pd.Series([0] * len(df))), errors='coerce').fillna(0)
    carries = pd.to_numeric(df.get('carries', pd.Series([0] * len(df))), errors='coerce').fillna(0)
    targets = pd.to_numeric(df.get('targets', pd.Series([0] * len(df))), errors='coerce').fillna(0)
    fg_attempts = pd.to_numeric(df.get('fg_attempts', pd.Series([0] * len(df))), errors='coerce').fillna(0)
    
    # Define starter criteria based on position group
    df['starter_flag'] = False
    
    # QBs are starters if they attempted 10+ passes
    qb_mask = (df['position_played'] == 'QB') & (attempts >= 10)
    # RBs are starters if they had 8+ carries
    rb_mask = (df['position_played'] == 'RB') & (carries >= 8)
    # WRs/TEs are starters if they had 4+ targets
    receiver_mask = (df['position_played'].isin(['WR', 'TE'])) & (targets >= 4)
    # Kickers are starters if they attempted any field goals or extra points
    kicker_mask = (df['position_played'] == 'K') & (fg_attempts > 0)
    
    # Combine all starter criteria
    df.loc[qb_mask | rb_mask | receiver_mask | kicker_mask, 'starter_flag'] = True
    
    # Define mapping from input column names to our database schema column names
    numeric_columns = {
        # Passing stats
        'attempts': 'passing_attempts',
        'completions': 'passing_completions',
        'passing_yards': 'passing_yards',
        'passing_tds': 'passing_tds',
        'interceptions': 'passing_ints',
        
        # Rushing stats
        'carries': 'rushing_attempts',
        'rushing_yards': 'rushing_yards',
        'rushing_tds': 'rushing_tds',
        
        # Receiving stats
        'targets': 'receiving_targets',
        'receptions': 'receptions',
        'receiving_yards': 'receiving_yards',
        'receiving_tds': 'receiving_tds',
        
        # Fumbles
        'sack_fumbles': 'fumbles',
        'rushing_fumbles': 'fumbles',
        'receiving_fumbles': 'fumbles',
        'sack_fumbles_lost': 'fumbles_lost',
        'rushing_fumbles_lost': 'fumbles_lost',
        'receiving_fumbles_lost': 'fumbles_lost',
        'fumbles': 'fumbles',
        'fumbles_lost': 'fumbles_lost',
        
        # Kicking stats
        'fg_attempts': 'field_goals_attempted',
        'fg_made': 'field_goals_made', 
        'pat_attempts': 'extra_points_attempted',
        'pat_made': 'extra_points_made',
        
        # Defensive stats
        'sacks': 'defensive_sacks',
        'tackles': 'defensive_tackles',
        'solo_tackles': 'solo_tackles',
        'assisted_tackles': 'assisted_tackles',
        'def_interceptions': 'defensive_interceptions',
        'fumbles_recovered': 'defensive_fumbles_recovered',
        'defensive_tds': 'defensive_tds',
        
        # Return stats
        'punt_returns': 'punt_returns',
        'punt_return_yards': 'punt_return_yards',
        'punt_return_tds': 'punt_return_tds',
        'kick_returns': 'kick_returns',
        'kick_return_yards': 'kick_return_yards',
        'kick_return_tds': 'kick_return_tds',
        'special_teams_tds': 'special_teams_tds'
    }
    
    # Initialize all numeric columns with zeros or None
    final_numeric_columns = set(numeric_columns.values())
    for col in final_numeric_columns:
        if col not in df.columns:
            df[col] = 0
    
    # Process each numeric column in a vectorized way
    for source_col, target_col in numeric_columns.items():
        if source_col in df.columns:
            # For fumbles and fumbles_lost, we need to aggregate them rather than overwrite
            if target_col in ['fumbles', 'fumbles_lost'] and df[target_col].sum() > 0:
                current_val = pd.to_numeric(df[target_col], errors='coerce').fillna(0)
                new_val = pd.to_numeric(df[source_col], errors='coerce').fillna(0)
                df[target_col] = (current_val + new_val).astype('Int64')
            else:
                df[target_col] = pd.to_numeric(df[source_col], errors='coerce').fillna(0).astype('Int64')
    
    # Special handling for defensive tackles if we have the component stats
    if 'solo_tackles' in df.columns and 'assisted_tackles' in df.columns:
        solo = pd.to_numeric(df['solo_tackles'], errors='coerce').fillna(0)
        assisted = pd.to_numeric(df['assisted_tackles'], errors='coerce').fillna(0)
        df['defensive_tackles'] = (solo + assisted).astype('Int64')
    
    # Format timestamp fields for BigQuery
    df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['updated_at'] = df['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Replace all NaN values with None for proper BigQuery handling
    df = df.replace({np.nan: None})
    
    # Define the final columns in the correct order to match table schema
    final_columns = [
        'stat_id',
        'player_id',
        'game_id',
        'team_id',
        'position_played',
        'snaps_played',
        'starter_flag',
        'passing_attempts',
        'passing_completions',
        'passing_yards',
        'passing_tds',
        'passing_ints',
        'rushing_attempts',
        'rushing_yards',
        'rushing_tds',
        'receiving_targets',
        'receptions',
        'receiving_yards',
        'receiving_tds',
        'fumbles',
        'fumbles_lost',
        'field_goals_attempted',
        'field_goals_made',
        'extra_points_attempted',
        'extra_points_made',
        'defensive_sacks',
        'defensive_tackles',
        'defensive_interceptions',
        'defensive_fumbles_recovered',
        'defensive_tds',
        'punt_returns',
        'punt_return_yards',
        'punt_return_tds',
        'kick_returns',
        'kick_return_yards',
        'kick_return_tds',
        'created_at',
        'updated_at'
    ]
    
    # Create the final DataFrame with only the columns we need
    result_df = pd.DataFrame(columns=final_columns)
    
    # Copy data from the transformed DataFrame to the result DataFrame
    for col in final_columns:
        if col in df.columns:
            result_df[col] = df[col]
        else:
            result_df[col] = None
    
    # Drop duplicates based on stat_id (should be unique per player+game)
    result_df = result_df.drop_duplicates(subset=['stat_id'])
    logger.info(f"Transformation complete. Returning DataFrame with {len(result_df)} rows")
    return result_df