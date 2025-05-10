import pandas as pd
import logging
import nfl_data_py as nfl
from datetime import datetime

# Configure logger
logger = logging.getLogger(__name__)

def collect_games_stadiums_teams_data(seasons: list[int] = None) -> tuple:
    """
    Collect NFL schedule data that can be used for Games, Stadiums, and Teams tables.
    
    Args:
        seasons: List of seasons to collect data for. If None, collects last 5 seasons.
        
    Returns:
        Tuple containing (games_df, teams_df)
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

    # Get team description data
    try:
        full_teams_df = nfl.import_team_desc()
        logger.info(f"Collected data for {len(full_teams_df)} teams")
        
        # Keep only the columns we need
        needed_cols = ['team_abbr', 'team_name', 'team_id', 'team_conf', 'team_division']
        teams_df = full_teams_df[needed_cols].copy()
        logger.info(f"Filtered teams data to only include needed columns: {needed_cols}")
        
        """ 
        Columns kept from nfl.import_team_desc():
        team_abbr - Team abbreviation (e.g., 'ARI', 'KC')
        team_name - Full team name (e.g., 'Arizona Cardinals')
        team_id - Team ID
        team_conf - Conference (e.g., 'NFC', 'AFC')
        team_division - Division (e.g., 'NFC West', 'AFC East')
        """
    except Exception as e:
        logger.error(f"Error collecting team descriptions: {e}")
        teams_df = pd.DataFrame()

    return games_df, teams_df

if __name__ == "__main__":
    collect_games_stadiums_teams_data()