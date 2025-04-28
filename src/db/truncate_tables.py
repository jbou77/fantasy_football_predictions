from google.cloud import bigquery
from src.utils.bigquery_helpers import get_bq_client
import logging

# Configure logger
logger = logging.getLogger(__name__)

class TableNotEmptyError(Exception):
    """Exception raised when a table should be empty but isn't."""
    pass

def truncate_table(project_id: str, dataset_id: str, table_name: str) -> bool:
    """
    Clear all data from a BigQuery table without deleting the table structure.
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_name: Name of the table to truncate
        
    Returns:
        bool: True if successful, False otherwise
    """
    client = get_bq_client()
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    # Using TRUNCATE TABLE which is more efficient for full table clearing
    query = f"TRUNCATE TABLE `{table_id}`"
    
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for query to finish
        logger.info(f"Table {table_id} truncated successfully.")
        return True
    except Exception as e:
        logger.error(f"Error truncating table: {e}")
        # If TRUNCATE fails (maybe older BigQuery version), try DELETE
        try:
            delete_query = f"DELETE FROM `{table_id}` WHERE true"
            delete_job = client.query(delete_query)
            delete_job.result()
            logger.info(f"Table {table_id} cleared using DELETE instead.")
            return True
        except Exception as delete_err:
            logger.error(f"Failed to clear table using DELETE as well: {delete_err}")
            return False

def verify_table_empty(project_id: str, dataset_id: str, table_name: str, fail_if_not_empty: bool = True) -> int:
    """
    Verify that a BigQuery table is empty.
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_name: Name of the table to check
        fail_if_not_empty: If True, raises an exception if the table is not empty
        
    Returns:
        int: Number of rows in the table (0 if empty)
        
    Raises:
        TableNotEmptyError: If fail_if_not_empty is True and the table contains rows
    """
    client = get_bq_client()
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    query = f"SELECT COUNT(*) as count FROM `{table_id}`"
    query_job = client.query(query)
    results = list(query_job.result())
    
    count = results[0].count
    if count > 0:
        error_msg = f"Table {table_id} contains {count} rows after truncation attempt"
        if fail_if_not_empty:
            logger.error(error_msg)
            raise TableNotEmptyError(error_msg)
        else:
            logger.warning(f"WARNING: {error_msg}")
    else:
        logger.info(f"Confirmed: Table {table_id} is empty.")
    
    return count