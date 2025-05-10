import pandas as pd
import logging
from datetime import datetime
import numpy as np

# Configure logger
logger = logging.getLogger(__name__)

def transform_teams_data(games_df: pd.DataFrame, teams_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform team data using NFL team descriptions and games dataframe.
    
    Args:
        games_df: Raw game DataFrame from NFL data source
        teams_df: Team descriptions DataFrame from nfl.import_team_desc()
        
    Returns:
        Transformed DataFrame ready for Teams table upload
    """
    logger.debug("Starting transformation of team data")
    
    # Get all unique team abbreviations from games
    home_teams = games_df[['home_team']].rename(columns={'home_team': 'team_abbreviation'})
    away_teams = games_df[['away_team']].rename(columns={'away_team': 'team_abbreviation'})
    
    # Combine and get unique teams from games
    teams_from_games = pd.concat([home_teams, away_teams]).drop_duplicates().reset_index(drop=True)
    team_abbrs_in_games = set(teams_from_games['team_abbreviation'])
    
    logger.info(f"Found {len(team_abbrs_in_games)} unique teams in games data")
    
    # Get stadium mapping
    team_stadium_map = {}
    if 'stadium_id' in games_df.columns and 'home_team' in games_df.columns:
        for idx, row in games_df.drop_duplicates(['home_team']).iterrows():
            if pd.notna(row['stadium_id']):
                team_stadium_map[row['home_team']] = row['stadium_id']
    
    # Hard-coded mapping for team cities (to ensure we have all 32 teams)
    team_city_map = {
        'ARI': 'Arizona',
        'ATL': 'Atlanta',
        'BAL': 'Baltimore',
        'BUF': 'Buffalo',
        'CAR': 'Carolina',
        'CHI': 'Chicago',
        'CIN': 'Cincinnati',
        'CLE': 'Cleveland',
        'DAL': 'Dallas',
        'DEN': 'Denver',
        'DET': 'Detroit',
        'GB': 'Green Bay',
        'HOU': 'Houston',
        'IND': 'Indianapolis',
        'JAX': 'Jacksonville',
        'KC': 'Kansas City',
        'LA': 'Los Angeles',
        'LAC': 'Los Angeles',
        'LV': 'Las Vegas',
        'MIA': 'Miami',
        'MIN': 'Minnesota',
        'NE': 'New England',
        'NO': 'New Orleans',
        'NYG': 'New York',
        'NYJ': 'New York',
        'PHI': 'Philadelphia',
        'PIT': 'Pittsburgh',
        'SEA': 'Seattle',
        'SF': 'San Francisco',
        'TB': 'Tampa Bay',
        'TEN': 'Tennessee',
        'WAS': 'Washington'
    }
    
    # Process teams data
    team_data = []
    
    # First, check if teams_df is valid and has the necessary columns
    if not teams_df.empty and 'team_abbr' in teams_df.columns and 'team_id' in teams_df.columns:
        logger.info(f"Using data from nfl.import_team_desc() for {len(teams_df)} teams")
        
        # Use teams_df as our primary source
        for _, row in teams_df.iterrows():
            team_abbr = row['team_abbr']
            
            # Only include teams that appear in our games
            if team_abbr in team_abbrs_in_games:
                team_record = {
                    'team_id': str(row['team_id']).zfill(4),
                    'team_abbreviation': team_abbr,
                    'team_name': row.get('team_name', None),
                    'team_city': team_city_map.get(team_abbr, None)
                }
                
                # Extract conference (AFC/NFC)
                conference = row.get('team_conf', None)
                team_record['conference'] = conference
                
                # Extract division (East/West/North/South)
                division_full = row.get('team_division', '')
                division = None
                if isinstance(division_full, str):
                    for div in ["East", "West", "North", "South"]:
                        if div in division_full:
                            division = div
                            break
                team_record['division'] = division
                
                # Add stadium ID
                team_record['stadium_id'] = team_stadium_map.get(team_abbr, None)
                
                # Add timestamps
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                team_record['created_at'] = current_time
                team_record['updated_at'] = current_time
                
                team_data.append(team_record)
    else:
        logger.warning("Team description data is empty or missing required columns. Using fallback method.")
        
        # Fallback: use data from games
        for idx, row in teams_from_games.iterrows():
            team_abbr = row['team_abbreviation']
            team_record = {
                'team_id': team_abbr,  # Use abbr as ID in fallback
                'team_abbreviation': team_abbr,
                'team_name': f"{team_city_map.get(team_abbr, team_abbr)} Team",  # Fallback name
                'team_city': team_city_map.get(team_abbr, team_abbr),
                'conference': None,  # We don't have this info
                'division': None,    # We don't have this info
                'stadium_id': team_stadium_map.get(team_abbr, None),
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            team_data.append(team_record)
    
    # Convert to DataFrame
    result_df = pd.DataFrame(team_data)
    
    # Replace all NaN values with None for proper BigQuery handling
    result_df = result_df.replace({np.nan: None})
    
    # Select final columns in the correct order to match table schema
    final_columns = [
        'team_id',
        'team_name',
        'team_city',
        'team_abbreviation',
        'conference',
        'division',
        'stadium_id',
        'created_at',
        'updated_at'
    ]
    
    # Ensure we have all columns in the correct order
    for col in final_columns:
        if col not in result_df.columns:
            result_df[col] = None
    
    result_df = result_df[final_columns]
    
    logger.debug(f"Transformation complete. Returning DataFrame with {len(result_df)} rows")
    return result_df