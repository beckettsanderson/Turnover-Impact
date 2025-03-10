import pandas as pd
from nba_api.stats.endpoints import *
import json
from tqdm import tqdm
import re
import os
import requests
import time

def get_game_df(season = '2024-25'):
    # gets dataframe of all games in a specified seasons

    games = json.loads(leaguegamelog.LeagueGameLog(season=season).get_json())
    game_df = pd.DataFrame(games['resultSets'][0]['rowSet'], columns = games['resultSets'][0]['headers'])
    
    good_cols = ['SEASON_ID', 'GAME_ID', 'TEAM_ABBREVIATION', 'GAME_DATE']
    game_df = game_df[good_cols].groupby('GAME_ID').agg({
        'SEASON_ID': 'first',
        'TEAM_ABBREVIATION': list,
        'GAME_DATE': 'first'
    }).reset_index()
    return game_df

# Function to convert 'PT11M35.00S' to seconds as float
def convert_to_seconds(time_str):
    match = re.match(r'PT(?:(\d+)M)?([\d.]+)S', time_str)
    if match:
        minutes = int(match.group(1)) if match.group(1) else 0
        seconds = float(match.group(2)) if match.group(2) else 0
        return minutes * 60 + seconds
    return None  # Handle invalid cases

def get_play_df(game_id, timeout= 2):
  # gets dataframe of play by play froma game id and fixes the clock column

  df = playbyplayv3.PlayByPlayV3(game_id=game_id, timeout=2).get_data_frames()[0]
  df['clock'] = df['clock'].apply(convert_to_seconds)
  return df

def tov_processor(play_df, teams):
    # this processes turnovers from one game

    tov_cols = ['team', 'player_id', 'player', 'type', 'period', 'clock','gameId', 'actionNumber', 'next_pos_points', 'shot_clock', 'opp_team', 'dead_ball']


    tov_df = pd.DataFrame(columns = tov_cols)

    i = 0 

    while i < len(play_df):


        if play_df.loc[i, 'actionType'] == 'Turnover':

            # get some basic info
            tov_info = list(play_df.loc[i, ['teamTricode', 'personId', 'playerName', 'subType', 'period', 'clock', 'gameId', 'actionNumber']])
            k = i

            if len(tov_info[0]) == 0:

                team_id  = play_df.loc[i, 'personId']
                tov_info[0] = play_df[play_df['teamId'] == team_id].iloc[0,5]

            # get points other team scored on next possession
            next_pos_points = 0
            while True:

                i+=1

                # check for end of period or other team turnover
                if play_df.loc[i, 'actionType'] in ['period', 'Turnover']:
                    break

                # check for possession of team that committed turnover
                if play_df.loc[i, 'teamTricode'] == tov_info[0] and play_df.loc[i, 'actionType'] in ['Rebound', 'Made Shot', 'Missed Shot', 'Free Throw']:
                    break

                # check for free throws made
                if play_df.loc[i, 'teamTricode'] == tov_info[0] and play_df.loc[i, 'actionType'] == 'Foul':
                    i+=1
                    while play_df.loc[i, 'actionType'] == 'Free Throw':
                        if 'MISS' not in play_df.loc[i, 'description']:
                            next_pos_points += 1
                        i+=1
                    break

                #check for made basket
                if play_df.loc[i, 'actionType'] == 'Made Shot':
                    next_pos_points += play_df.loc[i, 'shotValue']

                    # check for and one
                    if i + 2 < len(play_df):
                        if (play_df.iloc[i + 2]['subType'] == 'Free Throw 1 of 1' and
                            play_df.iloc[i + 2]['teamTricode'] != tov_info[0] and
                            "MISS" not in play_df.iloc[i + 2]['description']):
                            next_pos_points += 1
                            break



            # find time in shot clock at turnover
            j = k-1
            shot_clock = 0
            end = False
            while True:

                # check for start of period
                if play_df.loc[j, 'actionType'] == 'period':
                        shot_clock = max(24 - (play_df.loc[j, 'clock'] - tov_info[5]), shot_clock)
                        break
                     
                # check for start of possession
                if play_df.loc[j, 'actionType'] in ['Turnover', 'Free Throw'] and play_df.loc[j, 'teamTricode'] != tov_info[0]:
                    shot_clock = max(24 - (play_df.loc[j, 'clock'] - tov_info[5]), shot_clock)
                    break

                if play_df.loc[j, 'actionType'] == 'Made Shot' and play_df.loc[j, 'teamTricode'] != tov_info[0]:

                    if (play_df.loc[j, 'clock'] <= 60) or (play_df.loc[j, 'clock'] <= 120 and play_df.loc[j, 'period'] >= 4):
                        shot_clock = max(24 - (play_df.loc[j, 'clock'] - tov_info[5]), shot_clock)
                        break

                    shot_clock = max(26.5 - (play_df.loc[j, 'clock'] - tov_info[5]), shot_clock)



                # check for defensive foul that resets to 14 seconds
                if play_df.loc[j, 'actionType'] == 'Foul' and play_df.loc[j, 'teamTricode'] == tov_info[0]:
                    shot_clock = max(14 - (play_df.loc[j, 'clock'] - tov_info[5]), shot_clock)


                # check for rebound
                if play_df.loc[j, 'actionType'] == 'Rebound':

                    k = j-1

                    while True:

                        # check defensive rebound
                        if play_df.loc[k, 'actionType'] in ['Missed Shot', 'Free Throw'] and play_df.loc[k, 'teamTricode'] != tov_info[0]:
                            shot_clock = max(24 - (play_df.loc[j, 'clock'] - tov_info[5]), shot_clock)
                            end = True
                            break
                        
                        # check offensive rebound
                        if play_df.loc[k, 'actionType'] in ['Missed Shot', 'Free Throw'] and play_df.loc[k, 'teamTricode'] == tov_info[0] and 'BLOCK' not in play_df.loc[k, 'description']:
                            shot_clock = max(14 - (play_df.loc[j, 'clock'] - tov_info[5]), shot_clock)
                            break

                        k -= 1

                if end:
                    break

                j -= 1

            if tov_info[3] == 'Shot Clock Turnover':
                shot_clock = 0

            tov_info += [next_pos_points, shot_clock]
            tov_info += [[x for x in teams if x != tov_info[0]][0]]

            if tov_info[3] in ['Bad Pass', 'Lost Ball']:
                tov_info += [0]
            else:
                tov_info += [1]

            tov_df.loc[len(tov_df),:] = tov_info

        # check for 2 consecutive turnovers
        if play_df.loc[i, 'actionType'] == 'Turnover':
            continue
        i+=1

    return tov_df

def get_play_video(game_id, event_id, download=True, p = True):
    headers = {
        'Host': 'stats.nba.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token': 'true',
        'Connection': 'keep-alive',
        'Referer': 'https://stats.nba.com/',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache'
    }

    url = 'https://stats.nba.com/stats/videoeventsasset?GameEventID={}&GameID={}'.format(
                event_id, game_id)
    r = requests.get(url, headers=headers)
    json = r.json()
    video_urls = json['resultSets']['Meta']['videoUrls']
    if p:
        playlist = json['resultSets']['playlist']
        video_event = {'video': video_urls[0]['lurl'], 'desc': playlist[0]['dsc']}
        print(video_event)

    if download:
        # File name to save the video
        file_name = f"videos/event_{event_id}_game_{game_id}.mp4"
        video_url = video_urls[0]['lurl']

        # Download the video
        video_response = requests.get(video_url, stream=True)
        with open(file_name, 'wb') as f:
            for chunk in video_response.iter_content(chunk_size=1024):
                f.write(chunk)
        
        print(f"Video downloaded successfully as {file_name}")