from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from src.config.settings import PROJECT_ID

def get_bq_client():
    """Initialize and return a BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)

def create_dataset_if_not_exists(project_id: str, dataset_id: str):
    """Create a BigQuery dataset if it doesn't already exist."""
    client = get_bq_client()
    dataset_ref = bigquery.Client(project=project_id).dataset(dataset_id)  # Correctly creating a DatasetReference

    try:
        client.get_dataset(dataset_ref)  # Check if dataset exists
        print(f"Dataset {dataset_id} already exists.")
    except NotFound:  # Catch the specific NotFound exception
        # Dataset doesn't exist, create it
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set the location if necessary
        client.create_dataset(dataset)  # Create the dataset
        print(f"Created dataset {dataset_id}.")

