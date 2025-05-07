import pandas as pd
import logging
from datetime import datetime
import numpy as np
import re

# Configure logger
logger = logging.getLogger(__name__)

def transform_stadiums_data(games_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform stadium data from games dataframe to match Stadiums table schema.
    
    Args:
        games_df: Raw game DataFrame from NFL data source
        
    Returns:
        Transformed DataFrame ready for Stadiums table upload
    """
    logger.debug("Starting transformation of stadium data")
    
    # Extract stadium-related columns
    stadium_columns = ['stadium_id', 'stadium', 'roof', 'surface']
    
    # Check which columns exist in the dataframe
    available_columns = [col for col in stadium_columns if col in games_df.columns]
    
    # Extract unique stadiums
    stadiums_df = games_df[available_columns].drop_duplicates(subset=['stadium_id'] if 'stadium_id' in available_columns else ['stadium']).reset_index(drop=True)
    
    # Add timestamps
    current_time = datetime.now()
    stadiums_df['created_at'] = current_time
    stadiums_df['updated_at'] = current_time
    
    # Process stadium ID
    if 'stadium_id' in stadiums_df.columns:
        stadiums_df['stadium_id'] = stadiums_df['stadium_id'].astype(str)
    else:
        # If no stadium_id, create one from the name
        stadiums_df['stadium_id'] = stadiums_df['stadium'].apply(
            lambda x: re.sub(r'[^a-zA-Z0-9]', '', str(x)).upper()[:8]
        )
    
    # Process stadium name
    if 'stadium' in stadiums_df.columns:
        stadiums_df['stadium_name'] = stadiums_df['stadium'].astype(str)
    else:
        stadiums_df['stadium_name'] = "Unknown"
    
    # Add placeholder columns needed for schema
    stadiums_df['city'] = None
    stadiums_df['state'] = None
    
    # Process dome flag based on roof information
    if 'roof' in stadiums_df.columns:
        stadiums_df['dome_flag'] = stadiums_df['roof'].apply(
            lambda x: True if pd.notna(x) and str(x).upper() in ['DOME', 'CLOSED', 'INDOOR', 'RETRACTABLE'] else False
        )
    else:
        stadiums_df['dome_flag'] = None
    
    # Process surface type
    if 'surface' in stadiums_df.columns:
        stadiums_df['surface_type'] = stadiums_df['surface'].astype(str)
    else:
        stadiums_df['surface_type'] = None
    
    # Add placeholder for geographic coordinates
    stadiums_df['latitude'] = None
    stadiums_df['longitude'] = None
    
    # Format timestamp fields for BigQuery
    stadiums_df['created_at'] = stadiums_df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    stadiums_df['updated_at'] = stadiums_df['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Replace all NaN values with None for proper BigQuery handling
    stadiums_df = stadiums_df.replace({np.nan: None})
    
    # Select final columns in the correct order to match table schema
    final_columns = [
        'stadium_id',
        'stadium_name',
        'city',
        'state',
        'dome_flag',
        'surface_type',
        'latitude',
        'longitude',
        'created_at',
        'updated_at'
    ]
    
    # Create the final DataFrame with the desired columns
    logger.debug("Creating final DataFrame with required columns")
    result_df = pd.DataFrame(columns=final_columns)
    
    # Copy data from the transformed DataFrame to the result DataFrame
    for col in final_columns:
        if col in stadiums_df.columns:
            result_df[col] = stadiums_df[col]
    
    logger.debug(f"Transformation complete. Returning DataFrame with {len(result_df)} rows")
    return result_df