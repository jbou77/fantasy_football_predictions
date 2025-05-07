import pandas as pd
import logging
import nfl_data_py as nfl
from datetime import datetime

# Configure logger
logger = logging.getLogger(__name__)

def collect_games_stadiums_teams_data(seasons: list[int] = None) -> pd.DataFrame:
    """
    Collect NFL schedule data that can be used for Games, Stadiums, and Teams tables.
    
    Args:
        seasons: List of seasons to collect data for. If None, collects last 5 seasons.
        
    Returns:
        DataFrame containing raw NFL schedule data
    """
    if seasons is None:
        current_year = datetime.now().year
        # Get the last 5 seasons
        seasons = list(range(current_year - 4, current_year + 1))
    
    logger.info(f"Collecting game data for seasons: {seasons}")
    
    # Use nfl_data_py to get schedules
    games_df = nfl.import_schedules(seasons)
    logger.info(f"Collected data for {len(games_df)} games")
    
    
    # Columns returned by nfl.import_schedules():
    # - game_id         - season         - game_type      - week
    # - gameday         - weekday        - gametime       - away_team
    # - away_score      - home_team      - home_score     - location
    # - result          - total          - overtime       - old_game_id
    # - gsis            - nfl_detail_id  - pfr            - pff
    # - espn            - ftn            - away_rest      - home_rest
    # - away_moneyline  - home_moneyline - spread_line    - away_spread_odds
    # - home_spread_odds - total_line    - under_odds     - over_odds
    # - div_game        - roof           - surface        - temp
    # - wind            - away_qb_id     - home_qb_id     - away_qb_name
    # - home_qb_name    - away_coach     - home_coach     - referee
    # - stadium_id      - stadium


    return games_df