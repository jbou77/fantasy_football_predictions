import pandas as pd
import logging
from datetime import datetime
import numpy as np

# Configure logger
logger = logging.getLogger(__name__)

def transform_games_data(games_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw game data into the format required for database storage.
    
    Args:
        games_df: Raw game DataFrame from NFL data source
        
    Returns:
        Transformed DataFrame ready for BigQuery upload
    """
    logger.debug("Starting transformation of game data")
    
    # Create a copy of the dataframe to avoid modifying the original
    df = games_df.copy()
    
    # Add timestamps
    current_time = datetime.now()
    df['created_at'] = current_time
    df['updated_at'] = current_time
    
    # Convert game_id to string to ensure consistent typing
    df['game_id'] = df['game_id'].astype(str)
    
    # Convert season and week to integers where possible
    df['season_year'] = pd.to_numeric(df['season'], errors='coerce').astype('Int64')
    df['week_number'] = pd.to_numeric(df['week'], errors='coerce').astype('Int64')
    
    # Process team abbreviations - these are what comes from the NFL API
    df['home_team_abbr'] = df['home_team'].astype(str)
    df['away_team_abbr'] = df['away_team'].astype(str)
    
    # Set team IDs to be the same as abbreviations for now
    # This will be a placeholder until we establish a proper team mapping
    # In the future, we would use a separate teams table to map abbreviations to IDs
    df['home_team_id'] = df['home_team_abbr']
    df['away_team_id'] = df['away_team_abbr']
    
    # Process date field
    df['game_date'] = pd.to_datetime(df['gameday'], errors='coerce')
    df['game_date'] = df['game_date'].dt.strftime('%Y-%m-%d')
    
    # Game time already exists as 'gametime'
    df['game_time'] = df['gametime']
    
    # Stadium ID - use stadium_id from the data if available
    if 'stadium_id' in df.columns:
        df['stadium_id'] = df['stadium_id'].astype(str)
    elif 'stadium' in df.columns:
        # If no stadium_id but stadium name exists, use that as a fallback
        df['stadium_id'] = df['stadium'].astype(str)
    
    # Primetime flag based on game time and weekday
    # First check if it's a primetime day (Thursday, Sunday night, Monday)
    if 'weekday' in df.columns:
        df['primetime_flag'] = df.apply(
            lambda row: True if (
                pd.notna(row['weekday']) and row['weekday'].upper() in ['THURSDAY', 'MONDAY'] or
                (pd.notna(row['gametime']) and '20:' in str(row['gametime']) or '19:' in str(row['gametime']))
            ) else False,
            axis=1
        )
    else:
        # Fallback to just the time if weekday is not available
        df['primetime_flag'] = df['gametime'].apply(
            lambda x: True if pd.notna(x) and any(
                t in str(x).upper() for t in ['20:', '19:']
            ) else False
        )
    
    # Divisional matchup flag - use div_game if available
    if 'div_game' in df.columns:
        df['divisional_matchup_flag'] = df['div_game'].astype(bool)
    else:
        df['divisional_matchup_flag'] = False
    
    # Home and away scores - convert to integers
    df['home_score'] = pd.to_numeric(df['home_score'], errors='coerce').astype('Int64')
    df['away_score'] = pd.to_numeric(df['away_score'], errors='coerce').astype('Int64')
    
    # Add QB information
    if 'home_qb_id' in df.columns:
        df['home_qb_id'] = df['home_qb_id'].astype(str)
    
    if 'away_qb_id' in df.columns:
        df['away_qb_id'] = df['away_qb_id'].astype(str)
    
    # Add betting odds fields - convert to float
    odds_fields = [
        'home_moneyline', 'away_moneyline', 'spread_line', 
        'home_spread_odds', 'away_spread_odds', 'total_line',
        'over_odds', 'under_odds'
    ]
    
    for field in odds_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')
    
    # Format timestamp fields for BigQuery
    df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['updated_at'] = df['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Replace all NaN values with None for proper BigQuery handling
    df = df.replace({np.nan: None})
    
    # Select final columns in the correct order to match table schema
    final_columns = [
        'game_id',
        'season_year',
        'week_number',
        'home_team_id',
        'home_team_abbr',
        'away_team_id',
        'away_team_abbr',
        'game_date',
        'game_time',
        'stadium_id',
        'primetime_flag',
        'divisional_matchup_flag',
        'home_score',
        'away_score',
        'home_qb_id',
        'away_qb_id',
        'home_moneyline',
        'away_moneyline',
        'spread_line',
        'home_spread_odds',
        'away_spread_odds',
        'total_line',
        'over_odds',
        'under_odds',
        'created_at',
        'updated_at'
    ]
    
    # Create the final DataFrame with the desired columns
    logger.debug("Creating final DataFrame with required columns")
    result_df = pd.DataFrame(columns=final_columns)
    
    # Copy data from the transformed DataFrame to the result DataFrame
    for col in final_columns:
        if col in df.columns:
            result_df[col] = df[col]
    
    logger.debug(f"Transformation complete. Returning DataFrame with {len(result_df)} rows")
    return result_df
