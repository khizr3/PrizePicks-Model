"""
This Program is intended to provide a glimpse into daily NBA player projections from the app Prize-Picks and conduct a
statistical analysis into each line to identify potential value spots and different angles and trends from which to
justify the selection.

Author: Khizr Ali Khizr89@gmail.com

Created: March 6th, 2022
"""
# Imports
import math  # Using floor method to get odds for above line
import sys  # used to exit the program
import time  # Using sleep() between thread requests
from datetime import date, datetime

import pandas as pd  # Using DataFrames to store and manipulate data
import requests  # Using GET to load data from some API
import unidecode  # Decode the player names to remove accents
from bs4 import BeautifulSoup  # Needed in get_player_position_list()
from scipy.stats import poisson  # using poisson odds as one metric
from nba_api.stats.static import players  # a list of players
from nba_api.stats.endpoints import playergamelogs, leaguegamelog  # methods from nba-api to get data
from selenium import webdriver  # Needed in get_dvp_ranking()
from selenium.webdriver.support.ui import Select  # Needed in get_dvp_ranking()
from selenium.webdriver.common.by import By  # Needed in get_dvp_ranking()
from selenium.webdriver.chrome.options import Options  # Needed in get_dvp_ranking()
from selenium.webdriver.chrome.service import Service  # Needed in get_dvp_ranking()
from webdriver_manager.chrome import ChromeDriverManager  # Needed in get_dvp_ranking()


# Modules
# Get the Prizepicks Projections (Starting Data)
# Method is GOOD
def get_prizepicks_projections():
    """ Returns a DataFrame of the PrizePicks Projections
    
    Parameters:
    -----------
        
    Returns
    ---------
    df: <pandas.DataFrame>
        A DataFrame of the player's projection data
    """
    # URL of the Prize Picks Projections page
    url = 'https://partner-api.prizepicks.com/projections?single_stat=True&league_id=7&per_page=1000'
    resp = requests.get(url).json()
    if len(resp['data']) != 0:
        # Normalizes the JSON File into a Data Frame
        data = pd.json_normalize(resp['data'], max_level=3)
        included = pd.json_normalize(resp['included'], max_level=3)
        inc_cop = included[included['type'] == 'new_player'].copy().dropna(axis=1)

        # Joins on the 'id' to add the player name to the projections
        data = pd.merge(data, inc_cop,
                        how='left',
                        left_on=['relationships.new_player.data.id', 'relationships.new_player.data.type'],
                        right_on=['id', 'type'],
                        suffixes=('', '_new_player'))

        # Return the data with necessary columns
        data = data.rename(
            columns={'attributes.name': 'name', 'attributes.line_score': 'line_score',
                     'attributes.stat_type': 'stat_type', 'attributes.updated_at': 'updated_at',
                     'attributes.description': 'opponent', 'attributes.start_time': 'start_time',
                     'attributes.is_promo': 'is_promo', 'attributes.position': 'position', 'attributes.team': 'team',
                     'attributes.team_name': 'team_name', 'attributes.market': 'market'})
        return data[['id', 'name', 'line_score', 'stat_type', 'updated_at',
                     'opponent', 'start_time', 'is_promo', 'position',
                     'team', 'team_name', 'market']]
    else:
        print('There Are Currently no NBA Lines Available.')
        sys.exit()


# Get the DVP Rankings (Starting Data)
# Method is GOOD
def get_dvp_rankings():
    """ Returns a pandas.DataFrame of the NBA DVP Rankings from the last 30 days

        Parameters:
        -----------

        Returns
        ---------
        dvp_list: <pandas.DataFrame>
            A DataFrame of each team dvp and its position
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    s = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s, options=chrome_options)
    driver.get('https://hashtagbasketball.com/nba-defense-vs-position')
    select = Select(driver.find_element(By.NAME, 'ctl00$ContentPlaceHolder1$DDDURATION'))
    select.select_by_value("30")
    time.sleep(.500)
    table = driver.find_element(By.ID, 'ContentPlaceHolder1_GridView1').get_attribute('outerHTML')
    dvp_table = pd.read_html(table)[0]
    dvp_table = dvp_table.rename(
        columns={'Sort: Team': 'Team', 'Sort: Position': 'Position', 'Sort: PTS': 'PTS', 'Sort: FG%': 'FG%',
                 'Sort: FT%': 'FT%', 'Sort: 3PM': '3PM', 'Sort: REB': 'REB', 'Sort: AST': 'AST', 'Sort: STL': 'STL',
                 'Sort: BLK': 'BLK', 'Sort: TO': 'TO'})

    dvp_table['Team'] = dvp_table['Team'].str[:3]
    dvp_table['PTS'] = dvp_table['PTS'].str[:4]
    dvp_table['FG%'] = dvp_table['FG%'].str[:4]
    dvp_table['FT%'] = dvp_table['FT%'].str[:4]
    dvp_table['3PM'] = dvp_table['3PM'].str[:3]
    dvp_table['REB'] = dvp_table['REB'].str[:-3]
    dvp_table['AST'] = dvp_table['AST'].str[:-3]
    dvp_table['STL'] = dvp_table['STL'].str[:3]
    dvp_table['BLK'] = dvp_table['BLK'].str[:3]
    dvp_table['TO'] = dvp_table['TO'].str[:3]

    dvp_table['PTS'] = pd.to_numeric(dvp_table['PTS'])
    dvp_table['FG%'] = pd.to_numeric(dvp_table['FG%'])
    dvp_table['FT%'] = pd.to_numeric(dvp_table['FT%'])
    dvp_table['3PM'] = pd.to_numeric(dvp_table['3PM'])
    dvp_table['REB'] = pd.to_numeric(dvp_table['REB'])
    dvp_table['AST'] = pd.to_numeric(dvp_table['AST'])
    dvp_table['STL'] = pd.to_numeric(dvp_table['STL'])
    dvp_table['BLK'] = pd.to_numeric(dvp_table['BLK'])
    dvp_table['TO'] = pd.to_numeric(dvp_table['TO'])

    driver.close()
    return dvp_table


# Get a list of players and their position (Starting Data)
# Take a look at .replace() method issuing a warning
def get_player_position_list():
    """ Returns a pandas.DataFrame of the NBA DVP Rankings from the last 30 days

        Parameters:
        -----------

        Returns
        ---------
        player_info_list: <list>
            A list of players information including their position
    """
    url = 'https://www.basketball-reference.com/leagues/NBA_2022_per_game.html'
    r = requests.get(url)
    r_html = r.text
    soup = BeautifulSoup(r_html, 'html.parser')

    table = soup.find_all(class_="full_table")

    """ Extracting List of column names"""
    head = soup.find(class_="thead")
    column_names_raw = [head.text for _ in head][0]
    column_names_polished = column_names_raw.replace("\n", ",").split(",")[2:-1]

    """Extracting full list of player_data"""
    players_list = []

    for i in range(len(table)):

        player_ = []

        for td in table[i].find_all("td"):
            player_.append(unidecode.unidecode(td.text))

        players_list.append(player_)

    player_info_list = pd.DataFrame(players_list, columns=column_names_polished).set_index("Player")
    # cleaning the player's name from occasional special characters
    # player_info_list.index = player_info_list.index.str.encode('utf-8')
    player_info_list.index = player_info_list.index.str.replace('*', '', regex=True)
    return player_info_list


# Get a log of the last 10 games of all players listed in projections (Starting Data) (PrizePicks Projections Required)
# Look into changing the data to be streamlined and per player instead of all together
def get_game_logs(list_of_player_names):
    """ Returns Two Lists. 1. List of Player Names 2. list of pandas.DataFrame containing logs of the last 10 games

    Parameters:
    -----------
    list_of_projections: <pandas.DataFrame>
        A DataFrame containing the Prize Picks Projections

    Returns
    ---------
    player_names: <List>
        A List of the player's names
    game_logs: <List>
        A List of type pandas.DataFrame where each index contains the logs of the last 10 games played
    """
    player_names = []
    no_logs = []
    game_logs = []
    print('GETTING GAME LOGS')
    for name in list_of_player_names:
        if name not in player_names:
            player = players.find_players_by_full_name(name)[0]
            if player['is_active']:
                player_id = str(player['id'])
                player_logs = playergamelogs.PlayerGameLogs(date_from_nullable='10/31/2021',
                                                            player_id_nullable=player_id,
                                                            season_nullable='2021-22',
                                                            last_n_games_nullable=10).get_data_frames()[0]
                player_names.append(name)
                player_logs['ID'] = player_id
                game_logs.append(player_logs)
                time.sleep(.700)
                print(name, ' added to logs')
            else:
                no_logs.append(name)
    game_logs = pd.concat(game_logs, axis=0)
    print('FINISHED ALL ELIGIBLE GAME LOGS', no_logs)
    return game_logs, no_logs


def get_player_position(position_list, name):
    """ Returns Two Lists. 1. List of Player Names 2. list of pandas.DataFrame containing logs of the last 10 games

    Parameters:
    -----------
    list_of_projections: <pandas.DataFrame>
        A DataFrame containing the Prize Picks Projections

    Returns
    ---------
    player_names: <List>
        A List of the player's names
    game_logs: <List>
        A List of type pandas.DataFrame where each index contains the logs of the last 10 games played
    """
    if name == 'Robert Williams III':
        name = 'Robert Williams'

    return position_list[position_list.index == name]['Pos'][0]


# Get the Poisson Odds of the player going OVER their projected total given previous 10 performances
# Method is GOOD
def get_poisson_odds(game_log, prop_type, line_score):
    """ Returns a Value of the PrizePicks Projection Poisson odds to go over the suggested line_score

    Parameters:
    -----------
    game_log: <pandas.DataFrame>
        A DataFrame containing the players l10 Game Logs
    prop_type: <String>
        A string containing the type of prop it is eg. Points, Rebounds, Assists, etc.
    line_score: <float>
        A float value containing the line prize picks has set for the player and prop_type

    Returns
    ---------
    prob: <float>
        A float value of the player's poisson odds to go over the prize picks line
    """
    mean = 0
    if prop_type == 'Points':
        mean = game_log['PTS'].mean()
    elif prop_type == 'Rebounds':
        mean = game_log['REB'].mean()
    elif prop_type == 'Assists':
        mean = game_log['AST'].mean()
    elif prop_type == 'Pts+Rebs+Asts':
        mean = game_log['PTS'].mean() + game_log['REB'].mean() + game_log['AST'].mean()
    elif prop_type == '3-PT Made':
        mean = game_log['FG3M'].mean()
    elif prop_type == 'Fantasy Score':
        mean = game_log['NBA_FANTASY_PTS'].mean()
    elif prop_type == 'Blks+Stls':
        mean = game_log['BLK'].mean() + game_log['STL'].mean()
    elif prop_type == 'Free Throws Made':
        mean = game_log['FTM'].mean()
    elif prop_type == 'Free Throws Made':
        mean = game_log.count(game_log['PTS'] >= 10 and game_log['REB'] >= 10)/10
    else:
        print(prop_type)
    prob = 1 - poisson.cdf(k=math.floor(float(line_score) + 1), mu=mean)

    return prob


# Need to edit for singular piece of data
def get_dvp_odds(dvp_rankings, previous_opponents, opponent, player_position, prop_type):
    """ Returns a List of the PrizePicks Projections Poisson odds to go over

        Parameters:
        -----------
        list_of_projections: <pandas.DataFrame>
            A DataFrame containing the Prize Picks Projections

        Returns
        ---------
        prop_projections: <List>
            A List of the player's poisson odds to go over the prize picks line
    """
    if 'OKC' in previous_opponents:
        previous_opponents.append('OKL')
    if 'BKN' in previous_opponents:
        previous_opponents.append('BRO')

    if 'OKC' == opponent:
        opponent = 'OKL'
    if 'BKN' == opponent:
        opponent = 'BRO'

    if player_position == 'PG-SG' or player_position == 'SG-PG':
        temp_dvp = dvp_rankings[((dvp_rankings.Position == 'PG') | (dvp_rankings.Position == 'SG'))]
    elif player_position == 'SG-SF' or player_position == 'SF-SG':
        temp_dvp = dvp_rankings[((dvp_rankings.Position == 'SG') | (dvp_rankings.Position == 'SF'))]
    elif player_position == 'SF-PF' or player_position == 'PF-SF':
        temp_dvp = dvp_rankings[((dvp_rankings.Position == 'SG') | (dvp_rankings.Position == 'SF'))]
    elif player_position == 'PF-C' or player_position == 'C-PF':
        temp_dvp = dvp_rankings[((dvp_rankings.Position == 'C') | (dvp_rankings.Position == 'PF'))]
    else:
        temp_dvp = dvp_rankings[(dvp_rankings.Position == player_position)]

    temp_dvp['PRA'] = temp_dvp[['PTS', 'REB', 'AST']].sum(axis=1)
    temp_dvp['FS'] = temp_dvp.PTS + 1.2*temp_dvp.REB + 1.5*temp_dvp.AST + 3*temp_dvp.BLK + 3*temp_dvp.STL - temp_dvp.TO
    temp_dvp['B+S'] = temp_dvp['BLK'] + temp_dvp['STL']
    result = temp_dvp[(temp_dvp.Team == opponent)]
    temp_dvp = temp_dvp[(temp_dvp.Team.isin(previous_opponents))]
    print(opponent)
    print(result)
    # print(temp_dvp)
    if prop_type == 'Points':
        score = (result['PTS'] - temp_dvp['PTS'].mean()) / temp_dvp['PTS'].std()
    elif prop_type == 'Rebounds':
        print()
        score = (result['REB'] - temp_dvp['REB'].mean()) / temp_dvp['REB'].std()
    elif prop_type == 'Assists':
        score = (result['AST'] - temp_dvp['AST'].mean()) / temp_dvp['AST'].std()
    elif prop_type == 'Pts+Rebs+Asts':
        # pra = (result['PTS'] + result['REB'] + result['AST']).values[0]
        # print(type(pra), pra)
        score = ((result['PTS'] + result['REB'] + result['AST']) - temp_dvp['PRA'].mean()) / temp_dvp['PRA'].std()
    elif prop_type == '3-PT Made':
        score = (result['3PM'] - temp_dvp['3PM'].mean()) / temp_dvp['3PM'].std()
    elif prop_type == 'Fantasy Score':
        fantasy_score = result['PTS'] + 1.2 * result['REB'] + 1.5 * result['AST'] + 3 * result['BLK'] + 3 * result[
            'STL'] - result['TO']
        score = (fantasy_score - temp_dvp['FS'].mean()) / temp_dvp['FS'].std()
    elif prop_type == 'Blks+Stls':
        score = ((result['BLK'] + result['STL']) - temp_dvp['B+S'].mean()) / temp_dvp['B+S'].std()
    elif prop_type == 'Free Throws Made':
        score = (result['FT%'] - temp_dvp['FT%'].mean()) / temp_dvp['FT%'].std()
    else:
        print(prop_type)
        return 0
    print(score)
    return score.iloc[0]


def get_rest_situation(team_name):
    # i need to see how many games the team has played in last n days
    """

    :param team_name:
    :return:
    """
    """
    4 in 5 days
    3 in 4 days (played yesterday)
    2nd game of back to back
    3 in 4 days (rested yesterday)
    1 day rested
    2 days rested
    rested 3+ days
    datetime.strptime(player_log['GAME_DATE'].iat[0], "%Y-%m-%d").date()
    """
    game_logs = leaguegamelog.LeagueGameLog().get_data_frames()[0]
    time.sleep(.600)
    game_logs = game_logs[(game_logs['TEAM_ABBREVIATION'] == team_name)].iloc[-3:]
    game_dates = game_logs['GAME_DATE']
    most_recent_date = datetime.strptime(game_dates.iat[-1], "%Y-%m-%d").date()
    # print(most_recent_date, (most_recent_date - date.today()).days)
    if (date.today() - most_recent_date).days == 1:
        print(team_name, ' Played yesterday')
    elif (date.today() - most_recent_date).days == 2:
        prev_match_date = datetime.strptime(game_dates.iat[-2], "%Y-%m-%d").date()
        if (most_recent_date - prev_match_date).days == 1:
            return team_name + ' is playing their 3rd game in 4 days (Rested Yesterday)'
        else:
            return team_name + ' is playing with 1 day of rest'
    else:
        return team_name + ' is playing with 2+ days of rest'




def run():
    # Setting some options up
    pd.options.mode.chained_assignment = None
    pd.set_option('display.max_columns', 500)
    # Reading in all the required data
    prize_picks_projections = get_prizepicks_projections()  # a Dataframe of PrizePicks projections
    prize_picks_projections = prize_picks_projections[(prize_picks_projections['is_promo'] == False)]
    dvp_rankings = get_dvp_rankings()  # a Dataframe of all teams DVP ranking and value by position for last 30 Days
    player_position_list = get_player_position_list()
    l10_game_log = get_game_logs(prize_picks_projections['name'])  # a Dataframe of ALL players and their L10 game logs
    prize_picks_projections = prize_picks_projections[(~prize_picks_projections['name'].isin(l10_game_log[1]))]
    l10_game_log = l10_game_log[0]
    l10_game_log['GAME_DATE'] = l10_game_log['GAME_DATE'].str[:10]
    # Preparing Data Needed to iterate in loop
    model_info = pd.DataFrame()
    # Iterating through each projection and getting the metrics in a dataframe
    for projection in prize_picks_projections.itertuples():
        # Gathering required data from each projection
        # player_id = str(players.find_players_by_full_name(player_name)[0]['id'])
        print(projection)
        opponent = projection.opponent
        print(opponent)
        player_name = projection[2]
        player_log = l10_game_log[(l10_game_log['PLAYER_NAME'] == player_name)]
        if len(player_log) != 0:
            player_id = player_log['ID'][0]
            player_opps = player_log['MATCHUP'].str[-3:].tolist()
            player_line_score = projection.line_score
            player_pos = get_player_position(player_position_list, player_name)
            stat_type = projection.stat_type
            # days_rest = get_rest_situation(opponent)
            # Getting the first poisson odds metric
            poisson_odds = get_poisson_odds(game_log=player_log, prop_type=stat_type, line_score=player_line_score)
            print(player_name, player_pos)
            dvp_metric = get_dvp_odds(dvp_rankings=dvp_rankings, opponent=opponent, previous_opponents=player_opps,
                                      player_position=player_pos, prop_type=stat_type)

            data_to_insert = pd.DataFrame([[player_id, opponent, #days_rest,
                                            player_pos, player_name, player_line_score,
                                            stat_type, poisson_odds, dvp_metric]])
            model_info = pd.concat([model_info, data_to_insert], axis=0, ignore_index=True)
            # print(model_info)
    model_info.columns = ['player_id', 'opponent', #'rest_days',
                          'position', 'name', 'set_line', 'projection_type',
                          'poisson_odds', 'dvp_metric']

    # abs_max_dvp = max(-model_info['dvp_metric'].min(), model_info['dvp_metric'].max())
    model_info['dvp_metric_scaled'] = model_info['dvp_metric'] / 4
    model_info['model_score'] = (model_info['poisson_odds'] + model_info['dvp_metric_scaled'] / 2.5)
    model_info = model_info.sort_values('name')
    return model_info


# Run if directly executed
if __name__ == '__main__':
    df = run()
    for data_row in df.itertuples():
        # if data_row[7] > 0.6 and data_row[8] > 0:
        print(data_row)
    # print(player_name, player_line_score, stat_type, poisson_odds, dvp_metric)
