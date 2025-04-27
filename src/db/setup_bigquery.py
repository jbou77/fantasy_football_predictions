from src.db.schema.create_players_table import create_players_table
from src.db.schema.create_teams_table import create_teams_table
from src.db.schema.create_games_table import create_games_table
# from schema.create_stadiums_table import create_stadiums_table
from src.utils.bigquery_helpers import create_dataset_if_not_exists
from src.config.settings import PROJECT_ID, DATASET_ID

def main():
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists(PROJECT_ID, DATASET_ID)

    # Create tables
    create_players_table(PROJECT_ID, DATASET_ID)
    create_teams_table(PROJECT_ID, DATASET_ID)
    create_games_table(PROJECT_ID, DATASET_ID)
    # create_stadiums_table(PROJECT_ID, DATASET_ID)

if __name__ == "__main__":
    main()