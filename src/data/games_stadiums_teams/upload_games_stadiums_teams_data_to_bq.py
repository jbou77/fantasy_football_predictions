import pandas as pd
import sys
import os
import logging
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.config.settings import DATASET_ID, PROJECT_ID
from src.utils.bigquery_helpers import get_bq_client

# Configure logger
logger = logging.getLogger(__name__)

def upload_games_data(games_df: pd.DataFrame) -> None:
    """
    Upload game data to BigQuery.
    
    Args:
        games_df: DataFrame containing processed game data
    """
    logger.info("Starting upload of games data to BigQuery...")
    
    # Initialize BigQuery client
    client = get_bq_client()

    # Define BigQuery dataset and table name
    table_id = f"{PROJECT_ID}.{DATASET_ID}.Games"

    # Prepare the data for BigQuery upload - ensure NaN values are replaced with None
    rows_to_insert = games_df.replace({np.nan: None}).to_dict(orient="records")
    logger.info(f"Prepared {len(rows_to_insert)} game rows for insertion")

    # Insert the rows into BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if errors == []:
        logger.info(f"Successfully uploaded {len(games_df)} games to BigQuery.")
    else:
        logger.error(f"Error uploading games: {errors}")

def upload_stadiums_data(stadiums_df: pd.DataFrame) -> None:
    """
    Upload stadium data to BigQuery.
    
    Args:
        stadiums_df: DataFrame containing processed stadium data
    """
    logger.info("Starting upload of stadiums data to BigQuery...")
    
    # Initialize BigQuery client
    client = get_bq_client()

    # Define BigQuery dataset and table name
    table_id = f"{PROJECT_ID}.{DATASET_ID}.Stadiums"

    # Prepare the data for BigQuery upload - ensure NaN values are replaced with None
    rows_to_insert = stadiums_df.replace({np.nan: None}).to_dict(orient="records")
    logger.info(f"Prepared {len(rows_to_insert)} stadium rows for insertion")

    # Insert the rows into BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if errors == []:
        logger.info(f"Successfully uploaded {len(stadiums_df)} stadiums to BigQuery.")
    else:
        logger.error(f"Error uploading stadiums: {errors}")

def upload_teams_data(teams_df: pd.DataFrame) -> None:
    """
    Upload team data to BigQuery.
    
    Args:
        teams_df: DataFrame containing processed team data
    """
    logger.info("Starting upload of teams data to BigQuery...")
    
    # Initialize BigQuery client
    client = get_bq_client()

    # Define BigQuery dataset and table name
    table_id = f"{PROJECT_ID}.{DATASET_ID}.Teams"

    # Prepare the data for BigQuery upload - ensure NaN values are replaced with None
    rows_to_insert = teams_df.replace({np.nan: None}).to_dict(orient="records")
    logger.info(f"Prepared {len(rows_to_insert)} team rows for insertion")

    # Insert the rows into BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if errors == []:
        logger.info(f"Successfully uploaded {len(teams_df)} teams to BigQuery.")
    else:
        logger.error(f"Error uploading teams: {errors}")