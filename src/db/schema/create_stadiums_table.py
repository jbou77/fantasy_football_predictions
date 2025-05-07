from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.config.settings import PROJECT_ID, DATASET_ID

def create_stadiums_table(project_id: str, dataset_id: str):
    """Create the Stadiums table in BigQuery if it doesn't already exist."""
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table("Stadiums")

    schema = [
        bigquery.SchemaField("stadium_id", "STRING", mode="REQUIRED",
                             description="Unique identifier for the stadium"),
        bigquery.SchemaField("stadium_name", "STRING",
                             description="Full name of the stadium"),
        bigquery.SchemaField("city", "STRING",
                             description="City where the stadium is located"),
        bigquery.SchemaField("state", "STRING",
                             description="State or region where the stadium is located"),
        bigquery.SchemaField("dome_flag", "BOOLEAN",
                             description="Indicates if the stadium is a dome/indoor facility"),
        bigquery.SchemaField("surface_type", "STRING",
                             description="Type of playing surface (e.g., grass, turf)"),
        bigquery.SchemaField("latitude", "FLOAT",
                             description="Geographic latitude coordinate"),
        bigquery.SchemaField("longitude", "FLOAT",
                             description="Geographic longitude coordinate"),
        bigquery.SchemaField("created_at", "TIMESTAMP",
                             description="Timestamp when this record was created in the database"),
        bigquery.SchemaField("updated_at", "TIMESTAMP",
                             description="Timestamp when this record was last updated in the database"),
    ]

    try:
        client.get_table(table_ref)  # Check if table exists
        print("Stadiums table already exists.")
    except NotFound:
        # Table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        # Add table description
        table.description = "NFL stadium information including location, playing surface, and facility details."
        table = client.create_table(table)
        print("Created Stadiums table.")

if __name__ == "__main__":
    create_stadiums_table(PROJECT_ID, DATASET_ID)