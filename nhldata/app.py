'''
	This is the NHL crawler.  

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run 
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.  
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime
import boto3
import requests
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f'{self.SCHEMA_HOST}/{self.VERSION_PREFIX}'


    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        ''' 
        returns a dict tree structure that is like
            "dates": [ 
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        '''
        return self._get(self._url('schedule'), {'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')})

    def boxscore(self, game_id) -> dict:
        '''
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            #... player infoh6
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home" 
                }
            }
        '''

        url = self._url(f'game/{game_id}/boxscore')
        return self._get(url)

    def _get(self, url, params=None):
        retry_strategy = Retry(
            total=25,
            status_forcelist=[413, 429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        response = requests.get(url, params=params, timeout=0.1)

        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'

class Storage():
    def __init__(self, dest_bucket, s3_client):
        self._s3_client = s3_client
        self.bucket = dest_bucket
        return None

    def store_game(self, date, gameid, game_data, away) -> bool:
        if away:
            key = f'{self.date}'+'/'+'{self.gameid}'+'_away_team.csv'
        elif not away:
            key = f'{self.date}'+'/'+'{self.gameid}'+'_home_team.csv'
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=game_data)
        return True

class Crawler():
    def __init__(self, api: NHLApi, storage: Storage):
        self.api = api
        self.storage = storage
        return None

    def crawl(self, startDate: datetime, endDate: datetime) -> None:
        schedule_data = self.api.schedule(startDate, endDate)
        all_games = schedule_data['dates'][0]['games']
        return (schedule_data, all_games)
                 
if __name__ == "__main__":
    logger = logging.getLogger('START OF PIPELINE')
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    parser.add_argument("s_dt", type=str)
    parser.add_argument("e_dt", type=str)
    args = parser.parse_args()
    s3client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=os.environ.get('S3_ENDPOINT_URL'))

    startDate = datetime(year=int(args.s_dt[0:4]), month=int(args.s_dt[4:6]), day=int(args.s_dt[6:8]))
    endDate = datetime(year=int(args.e_dt[0:4]), month=int(args.e_dt[4:6]), day=int(args.e_dt[6:8]))

    dest_bucket = os.environ.get('DEST_BUCKET', 'output')
    api = NHLApi()
    storage_obj = Storage(dest_bucket, s3client)
    crawl_obj = Crawler(api, storage_obj)
    logger.info('calling API for schedule')
    (schedule_data, all_games) = crawl_obj.crawl(startDate, endDate)
    if schedule_data is not None:
        logger.info('successfully retrieved data from schedule API Call')

    for game in all_games:
        game_date = game['gameDate'][0:10]
        gameid = game['gamePk']
        logger.info('for game', gameid, 'calling API for boxscore')
        gamebox_data = api.boxscore(gameid)
        if gamebox_data is not None:
            logger.info('successfully retrieved data from schedule API Call')
        else:
            logger.info('gamebox data is none -- API Call was not successful')
        away_player_data = gamebox_data['teams']['away']['players']
        home_player_data = gamebox_data['teams']['home']['players']
        storage_obj.store_game(game_date, gameid, away_player_data, away=True)
        storage_obj.store_game(game_date, gameid, home_player_data, away=False)
    logger.info('successfully finished crawl')
    logger.info('END OF PIPELINE')
