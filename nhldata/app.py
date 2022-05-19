"""
	This is the NHL crawler.

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
"""
import logging
from io import StringIO
from typing import Any, Dict, List, Tuple, Optional
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import boto3
import requests
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)


class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f'{self.SCHEMA_HOST}/{self.VERSION_PREFIX}'

    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        """
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
        """
        return self._get(self._url('schedule'),
                         {'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')})

    def boxscore(self, game_id):
        """
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            "person": {
                                "id": $int,
                                "fullName": $string,
                                #-- other info
                                "currentTeam": {
                                    "name": $string,
                                    #-- other info
                                },
                                "stats": {
                                    "skaterStats": {
                                        "assists": $int,
                                        "goals": $int,
                                        #-- other status
                                    }
                                    #-- ignore "goalieStats"
                                }
                            }
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home"
                }
            }

            See tests/resources/boxscore.json for a real example response
        """
        url = self._url(f'game/{game_id}/boxscore')
        return self._get(url)

    def _get(self, url, params=None):
        response = requests.get(url, params=params, timeout=60)
        LOG.info(f'Sending get request to {url}')
        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'


@dataclass
class StorageKey:
    # TODO what properties are needed to partition?
    gameid: str

    def key(self):
        """ renders the s3 key for the given set of properties """
        # TODO use the properties to return the s3 key
        return f'{self.gameid}.csv'


class Storage:
    def __init__(self, dest_bucket, s3_client):
        """Initializes Storage class

        Args:
            dest_bucket: S3 bucket name
            s3_client: S3 Client
        """
        self._s3_client = s3_client
        self.bucket = dest_bucket

    def store_game(self, key: StorageKey, game_data) -> bool:
        """Converts start_date and end_date into datetimes

        Args:
            start_date: start date
            end_date: end date

        Returns:
            Formatted datetimes
        """
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=game_data)
        LOG.info(f'Storing key={key.key()} in {self.bucket}')
        return True


class Crawler:
    def __init__(self, api: NHLApi, storage: Storage):
        """Initializes Crawler class

        Args:
            api: API endpoint
            storage: Storage client
        """
        self.api = api
        self.storage = storage

    def parse_player_data(self, player_data: List[Dict]) -> "DataFrame":
        """Converts start_date and end_date into datetimes

        Args:
            player_data: List of player data

        Returns:
            Cleaned player data as DataFrame
        """
        player_data_df = pd.json_normalize(player_data)
        player_data_df = player_data_df.add_prefix('player_')
        player_data_df = player_data_df.rename(columns={'player_side': 'side'})
        player_data_df = player_data_df.rename(columns=lambda col: col.replace('.', '_'))
        player_data_df = player_data_df.astype(object).where(pd.notnull(player_data_df), None)

        return player_data_df

    def create_game_stats(self, game_data: Dict) -> "DataFrame":
        """Converts start_date and end_date into datetimes

        Args:
            game_data: start date

        Returns:
            Cleaned player data as DataFrame
        """
        player_data = []
        for team in game_data.get('teams'):
            for player in game_data['teams'][team]['players'].values():
                if 'Goalie' != player['person']['primaryPosition']['name']:  # ignore goalies
                    player['side'] = team
                    player_data.append(player)

        return self.parse_player_data(player_data)

    def crawl(self, start_date: datetime, end_date: datetime) -> None:
        """Fetches and stores all game data and player data for a specified date range

        Args:
            start_date: start date
            end_date: end date
        """
        # NOTE the data direct from the API is not quite what we want. Its nested in a way we don't want
        #      so here we are looking for your ability to gently massage a data set.
        # TODO error handling
        # TODO get games for dates
        # TODO for each game get all player stats: schedule -> date -> teams.[home|away] -> $playerId: player_object (see boxscore above)
        # TODO ignore goalies (players with "goalieStats")
        # TODO output to S3 should be a csv that matches the schema of utils/create_games_stats

        try:
            schedule = self.api.schedule(start_date=start_date, end_date=end_date)
        except Exception as err:
            raise Exception(f'Failed to get schedule for start_date={start_date}, end_date={end_date}')

        schedule_dates = schedule.get('dates')

        for schedule_date in schedule_dates:
            LOG.info(f'Fetching games from {schedule_date}')
            for game in schedule_date.get('games'):
                game_data = self.api.boxscore(game.get('gamePk'))
                game_data_df = self.create_game_stats(game_data)
                csv_buffer = StringIO()
                game_data_df.to_csv(csv_buffer, na_rep=None)
                self.storage.store_game(key=StorageKey(game['gamePk']), game_data=csv_buffer.getvalue())


def parse_crawl_args(start_date: str, end_date: str) -> Tuple[str, str]:
    """Converts start_date and end_date into datetimes

    Args:
        start_date: start date
        end_date: end date

    Returns:
        Formatted datetimes
    """
    datetime_format = '%Y-%m-%d'

    try:
        start_date = datetime.strptime(start_date, datetime_format)
    except ValueError as error:
        raise ValueError(f'ValueError invalid start_date. Use value like 2020-08-04') from error
    try:
        end_date = datetime.strptime(end_date, datetime_format)
    except ValueError as error:
        raise ValueError(f'Invalid start_date. Use value like 2020-08-04') from error

    if start_date >= end_date:
        raise ValueError(f'start_date must be less than end_date')
    return start_date, end_date


def main():
    """Grabs NHL data for a specified date range and stores to simulated S3 bucket"""
    import os
    import argparse
    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    # TODO what arguments are needed to make this thing run,  if any?
    parser.add_argument('--start_date', type=str, help='YYYY-MM-DD', default='2020-08-04')  # TODO remove default
    parser.add_argument('--end_date', type=str, help='YYYY-MM-DD', default='2020-08-05')  # TODO remove default
    args = parser.parse_args()
    start_date, end_date = parse_crawl_args(args.start_date, args.end_date)
    dest_bucket = os.environ.get('DEST_BUCKET', 'output')

    api = NHLApi()
    s3client = boto3.client('s3', config=Config(signature_version='s3v4'),
                            endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    storage = Storage(dest_bucket, s3client)
    crawler = Crawler(api, storage)
    crawler.crawl(start_date, end_date)


if __name__ == '__main__':
    main()
