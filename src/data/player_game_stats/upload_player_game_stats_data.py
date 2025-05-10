import pandas as pd
import sys
import os
import logging
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.utils.bigquery_helpers import get_bq_client
from src.config.settings import DATASET_ID, PROJECT_ID

# Configure logger
logger = logging.getLogger(__name__)

def upload_player_game_stats_data(stats_df: pd.DataFrame) -> None:
    """
    Upload player game statistics to BigQuery.
    
    Args:
        stats_df: DataFrame containing processed player game statistics
    """
    logger.info("Starting upload to BigQuery...")
    
    # Initialize BigQuery client
    client = get_bq_client()

    # Define BigQuery dataset and table name
    table_id = f"{PROJECT_ID}.{DATASET_ID}.PlayerGameStats"

    # Prepare the data for BigQuery upload - ensure NaN values are replaced with None
    rows_to_insert = stats_df.replace({np.nan: None}).to_dict(orient="records")
    logger.info(f"Prepared {len(rows_to_insert)} rows for insertion")

    # Insert the rows into BigQuery
    # For large datasets, consider using batch methods
    if len(rows_to_insert) > 10000:
        # Split into batches of 10000
        batch_size = 10000
        total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size
        
        for i in range(total_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(rows_to_insert))
            batch = rows_to_insert[start_idx:end_idx]
            
            errors = client.insert_rows_json(table_id, batch)
            if errors:
                logger.error(f"Errors in batch {i+1}/{total_batches}: {errors}")
            else:
                logger.info(f"Successfully uploaded batch {i+1}/{total_batches} ({len(batch)} records)")
    else:
        # Upload all at once for smaller datasets
        errors = client.insert_rows_json(table_id, rows_to_insert)
        
        if errors == []:
            logger.info(f"Successfully uploaded {len(stats_df)} player game stat records to BigQuery.")
        else:
            logger.error(f"Error uploading player game stats: {errors}")