import nfl_data_py as nfl

def collect_basic_player_data():
    players_df = nfl.import_players()
    
    # Filter down to active players
    players_df = players_df[players_df['status_short_description'] == 'Active']
    return players_df