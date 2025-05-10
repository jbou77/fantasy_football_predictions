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
    
    # Create a copy of the dataframe to avoid modifying the original
    df = weekly_df.copy()
    
    # Add timestamps
    current_time = datetime.now()
    df['created_at'] = current_time
    df['updated_at'] = current_time
    
    # Process player_id - ensure it's a string
    df['player_id'] = df['player_id'].astype(str)
    
    # Create game_id that matches the format in game_ids: "YYYY_WW_AWAY_HOME"
    if 'game_id' not in df.columns:
        # Process each row to determine correct game_id
        game_ids_list = []
        
        for idx, row in df.iterrows():
            season = str(row['season'])
            # Format week with leading zero if needed
            week = str(row['week']).zfill(2)
            team1 = row['recent_team']
            team2 = row['opponent_team']
            
            # Try both combinations to determine home/away
            potential_game_id_1 = f"{season}_{week}_{team1}_{team2}"
            potential_game_id_2 = f"{season}_{week}_{team2}_{team1}"
            
            # Check if either potential game_id exists in the provided set
            if potential_game_id_1 in game_ids:
                # In this case, team1 is away, team2 is home
                game_ids_list.append(potential_game_id_1)
            elif potential_game_id_2 in game_ids:
                # In this case, team2 is away, team1 is home
                game_ids_list.append(potential_game_id_2)
            else:
                # If we can't find a match, make our best guess based on a convention
                # where we'll assume the player's team is listed first when they're away
                logger.warning(f"No matching game_id found for {season}, week {week}, {team1} vs {team2}. Creating best guess.")
                game_ids_list.append(f"unknown_{season}_{week}_{team1}_{team2}")
                
        df['game_id'] = game_ids_list
    
    df['game_id'] = df['game_id'].astype(str)
    
    # Create a unique stat_id for each record (player_id + game_id combination)
    df['stat_id'] = df['player_id'].astype(str) + '_' + df['game_id'].astype(str)
    
    # Process team_id - use recent_team as team_id
    df['team_id'] = df['recent_team'].astype(str)
    
    # Process position
    df['position_played'] = df['position'].astype(str)
    
    # Handle snaps data - they might not be in the input data
    # If snaps data is missing, we'll leave it as None for now
    df['snaps_played'] = None
    
    # Process starter flag - using stats to infer starter status
    # For QBs, consider starter if they had at least 10 passing attempts
    attempts = pd.to_numeric(df.get('attempts', pd.Series([0] * len(df))), errors='coerce').fillna(0)
    carries = pd.to_numeric(df.get('carries', pd.Series([0] * len(df))), errors='coerce').fillna(0)
    targets = pd.to_numeric(df.get('targets', pd.Series([0] * len(df))), errors='coerce').fillna(0)
    
    # Define starter criteria based on position group
    df['starter_flag'] = False
    
    # QBs are starters if they attempted 10+ passes
    qb_mask = (df['position'] == 'QB') & (attempts >= 10)
    # RBs are starters if they had 8+ carries
    rb_mask = (df['position'] == 'RB') & (carries >= 8)
    # WRs/TEs are starters if they had 4+ targets
    receiver_mask = (df['position'].isin(['WR', 'TE'])) & (targets >= 4)
    
    # Combine all starter criteria
    df.loc[qb_mask | rb_mask | receiver_mask, 'starter_flag'] = True
    
    # Define mapping from input column names to our database schema column names
    # Most of these names already match from the sample data
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
        
        # Defensive stats - these might not be in the sample data
        'sacks': 'defensive_sacks',
        'tackles': 'defensive_tackles',
        
        # Fumbles
        'sack_fumbles': 'fumbles',
        'rushing_fumbles': 'fumbles',
        'receiving_fumbles': 'fumbles',
        'sack_fumbles_lost': 'fumbles_lost',
        'rushing_fumbles_lost': 'fumbles_lost',
        'receiving_fumbles_lost': 'fumbles_lost',
        
        # Kicking stats - might not be in sample data
        'fg_attempts': 'field_goals_attempted',
        'fg_made': 'field_goals_made', 
        'pat_attempts': 'extra_points_attempted',
        'pat_made': 'extra_points_made',
        
        # More defensive stats
        'solo_tackles': 'solo_tackles',
        'assisted_tackles': 'assisted_tackles',
        'interceptions_defense': 'defensive_interceptions',
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
    
    # Initialize all numeric columns with zeros
    for _, target_col in numeric_columns.items():
        if target_col not in df.columns:
            df[target_col] = 0
    
    # Process each numeric column in a vectorized way
    for source_col, target_col in numeric_columns.items():
        if source_col in df.columns:
            # For fumbles and fumbles_lost, we need to aggregate them rather than overwrite
            if target_col in ['fumbles', 'fumbles_lost']:
                current_val = pd.to_numeric(df.get(target_col, pd.Series([0] * len(df))), errors='coerce').fillna(0)
                new_val = pd.to_numeric(df[source_col], errors='coerce').fillna(0)
                df[target_col] = (current_val + new_val).astype('Int64')
            else:
                df[target_col] = pd.to_numeric(df[source_col], errors='coerce').fillna(0).astype('Int64')
    
    # Special handling for defensive tackles if needed
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
    result_columns = [col for col in final_columns if col in df.columns]
    result_df = df[result_columns].copy()
    
    # Check for missing columns and add them
    missing_columns = set(final_columns) - set(result_columns)
    for col in missing_columns:
        result_df[col] = None
    
    # Ensure columns are in the correct order
    result_df = result_df[final_columns]
    
    logger.debug(f"Transformation complete. Returning DataFrame with {len(result_df)} rows")
    return result_df