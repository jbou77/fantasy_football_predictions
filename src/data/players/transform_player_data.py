import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Configure logger
logger = logging.getLogger(__name__)

def transform_player_data(players_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw player data into the format required for database storage.
    
    Args:
        players_df: Raw player DataFrame from NFL data source
        
    Returns:
        Transformed DataFrame ready for BigQuery upload
    """
    # Select the necessary columns that exist in the DataFrame
    logger.debug("Selecting necessary columns from raw data")
    players_df = players_df[[ 
        'gsis_id',                  # player_id
        'first_name',
        'last_name',
        'position',
        'current_team_id',          # team_id
        'birth_date',
        'height',
        'weight',
        'college_name',             # college
        'status_short_description', # active_status
        'entry_year',               # draft_year from entry_year
        'draft_number'              # draft_position from draft_number
    ]].copy()

    # Add timestamps
    logger.debug("Adding timestamp fields")
    current_time = datetime.now()
    players_df['created_at'] = current_time
    players_df['updated_at'] = current_time

    # Convert columns to appropriate data types
    logger.debug("Converting data types")
    players_df['player_id'] = players_df['gsis_id'].astype(str)
    
    # Process date fields
    logger.debug("Processing date fields")
    players_df['birth_date'] = pd.to_datetime(players_df['birth_date'], errors='coerce')
    players_df['birth_date'] = players_df['birth_date'].dt.strftime('%Y-%m-%d')

    # Process numeric fields
    logger.debug("Processing numeric fields")
    players_df['height'] = pd.to_numeric(players_df['height'], errors='coerce')
    players_df['weight'] = pd.to_numeric(players_df['weight'], errors='coerce')
    players_df['draft_year'] = players_df['entry_year']
    players_df['draft_position'] = players_df['draft_number']
    
    # Process boolean fields
    logger.debug("Processing boolean fields")
    players_df['active_status'] = players_df['status_short_description'] == 'Active'

    # Format timestamp fields
    logger.debug("Formatting timestamp fields")
    players_df['created_at'] = players_df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    players_df['updated_at'] = players_df['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Replace all NaN values with None
    logger.debug("Replacing NaN values with None")
    players_df = players_df.replace({np.nan: None})

    # Rename columns to match BigQuery schema
    logger.debug("Renaming columns to match DB schema")
    players_df = players_df.rename(columns={
        'current_team_id': 'team_id',
        'college_name': 'college'
    })
    
    # Select final columns in the correct order
    logger.debug("Selecting final columns")
    players_df = players_df[[ 
        'player_id',             
        'first_name',
        'last_name',
        'position',
        'team_id',       
        'birth_date',
        'height',
        'weight',
        'college',         
        'draft_year',
        'draft_position',
        'active_status',         
        'created_at',
        'updated_at'
    ]]
    
    return players_df