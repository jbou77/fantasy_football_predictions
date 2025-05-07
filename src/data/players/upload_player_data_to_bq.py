import pandas as pd
import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.utils.bigquery_helpers import get_bq_client
from src.config.settings import DATASET_ID, PROJECT_ID
import numpy as np


# Configure logger
logger = logging.getLogger(__name__)
def upload_basic_player_data(players_df: pd.DataFrame) -> None:
    """
    Upload player data to BigQuery.
    
    Args:
        players_df: DataFrame containing processed player data
    """
    logger.info("Starting upload to BigQuery...")
    
    # Initialize BigQuery client
    client = get_bq_client()

    # Define BigQuery dataset and table name
    table_id = f"{PROJECT_ID}.{DATASET_ID}.Players"

    # Prepare the data for BigQuery upload - ensure NaN values are replaced with None
    rows_to_insert = players_df.replace({np.nan: None}).to_dict(orient="records")
    logger.info(f"Prepared {len(rows_to_insert)} rows for insertion")

    # Insert the rows into BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if errors == []:
        logger.info(f"Successfully uploaded {len(players_df)} players to BigQuery.")
    else:
        logger.error(f"Error uploading players: {errors}")
