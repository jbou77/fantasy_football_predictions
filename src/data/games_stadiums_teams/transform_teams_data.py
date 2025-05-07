import pandas as pd
import logging
from datetime import datetime
import numpy as np

# Configure logger
logger = logging.getLogger(__name__)

def transform_teams_data(games_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform team data from games dataframe to match Teams table schema.
    
    Args:
        games_df: Raw game DataFrame from NFL data source
        
    Returns:
        Transformed DataFrame ready for Teams table upload
    """
    logger.debug("Starting transformation of team data")
    
    # Get all unique team abbreviations
    home_teams = games_df[['home_team']].rename(columns={'home_team': 'team_abbreviation'})
    away_teams = games_df[['away_team']].rename(columns={'away_team': 'team_abbreviation'})
    
    # Combine and get unique teams
    all_teams = pd.concat([home_teams, away_teams]).drop_duplicates().reset_index(drop=True)
    
    # Add timestamps
    current_time = datetime.now()
    all_teams['created_at'] = current_time
    all_teams['updated_at'] = current_time
    
    # Set team_id to be the same as team_abbreviation for now
    all_teams['team_id'] = all_teams['team_abbreviation']
    
    # Try to extract team information if available
    team_info = {}
    
    # Attempt to get conference and division info from a subset of recent games
    # This is a simplistic approach - real implementation would require better data
    # We can extract this information more reliably from other nfl_data_py functions
    # but keeping it simple for this example
    
    # Fill in basic team information
    all_teams['team_name'] = None  # Would need mapping of abbreviations to names
    all_teams['team_city'] = None  # Would need mapping of abbreviations to cities
    all_teams['conference'] = None  # Would need mapping or analysis
    all_teams['division'] = None   # Would need mapping or analysis
    
    # Map teams to their primary stadiums
    # This is a simplistic approach - teams may play in multiple stadiums
    team_stadium_map = {}
    
    # Look at home games to map teams to stadiums
    if 'stadium_id' in games_df.columns and 'home_team' in games_df.columns:
        for idx, row in games_df.drop_duplicates(['home_team']).iterrows():
            team_stadium_map[row['home_team']] = row['stadium_id']
    
    # Apply stadium mapping
    all_teams['stadium_id'] = all_teams['team_abbreviation'].map(team_stadium_map)
    
    # Format timestamp fields for BigQuery
    all_teams['created_at'] = all_teams['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    all_teams['updated_at'] = all_teams['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Replace all NaN values with None for proper BigQuery handling
    all_teams = all_teams.replace({np.nan: None})
    
    # Select final columns in the correct order to match table schema
    final_columns = [
        'team_id',
        'team_name',
        'team_city',
        'team_abbreviation',
        'conference',
        'division',
        'stadium_id',
        'created_at',
        'updated_at'
    ]
    
    # Create the final DataFrame with the desired columns
    logger.debug("Creating final DataFrame with required columns")
    result_df = pd.DataFrame(columns=final_columns)
    
    # Copy data from the transformed DataFrame to the result DataFrame
    for col in final_columns:
        if col in all_teams.columns:
            result_df[col] = all_teams[col]
    
    logger.debug(f"Transformation complete. Returning DataFrame with {len(result_df)} rows")
    return result_df