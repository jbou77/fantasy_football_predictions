import pandas as pd
import numpy as np
from datetime import datetime
import logging

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
    
    # Add timestamps
    current_time = datetime.now()
    df['created_at'] = current_time
    df['updated_at'] = current_time
    
    # Process player_id - ensure it's a string
    df['player_id'] = df['player_id'].astype(str)
    
    # Process game_id - ensure it's a string
    df['game_id'] = df['game_id'].astype(str)
    
    # Create a unique stat_id for each record (player_id + game_id combination)
    df['stat_id'] = df['player_id'].astype(str) + '_' + df['game_id'].astype(str)
    
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
        'interceptions': 'defensive_interceptions',
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
    # result_df.to_csv('player_game_stats_transformed.csv', index=False)
    # Drop duplicates based on stat_id (should be unique per player+game)
    result_df = result_df.drop_duplicates(subset=['stat_id'])
    # result_df.to_csv('player_game_stats_transformed_deduped.csv', index=False)
    logger.info(f"Transformation complete. Returning DataFrame with {len(result_df)} rows")
    return result_df