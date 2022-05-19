import json
from datetime import datetime
from unittest import TestCase

from unittest.mock import patch

import boto3
from botocore.config import Config

from nhldata.app import Crawler, Storage

from nhldata.app import NHLApi, parse_crawl_args


class CrawlerTest(TestCase):
    @patch.object(Storage, 'store_game', side_effect=True)
    def setUp(self, mock_storage):
        """Initial setup of shared mocks and objects"""
        self.api = NHLApi()
        self.s3client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url='http://s3:9000')
        self.crawler = Crawler(self.api, mock_storage)
        self.boxscore = json.load(open('resources/boxscore.json'))
        self.gamePk = 2019030042

    def test_crawl(self):
        # Set Up
        start_date = '2020-08-04'
        end_date = '2020-08-05'
        start_datetime, end_datetime = parse_crawl_args(start_date, end_date)

        schedule = json.load(open('resources/test_schedule.json'))
        mock_schedule = patch.object(target=NHLApi, attribute='schedule')
        mock_schedule = mock_schedule.start()
        mock_schedule.return_value = schedule

        mock_api = patch.object(target=NHLApi, attribute='boxscore')
        mock_api = mock_api.start()
        mock_api.return_value = self.boxscore

        mock_crawler = patch.object(target=Crawler, attribute='create_game_stats')
        mock_crawler = mock_crawler.start()

        # Test
        self.crawler.crawl(start_date=start_date, end_date=end_date)

        # Verify
        mock_schedule.assert_called_once_with(start_date=start_date, end_date=end_date)
        mock_api.assert_called_once_with(self.gamePK)
        mock_crawler.assert_called_once_with(self.boxscore)

    def test_create_game_stats(self):
        # Set Up
         # Test
        actual_game_stats = self.crawler.create_game_stats(self.boxscore)

        # Verify
        assert 'G' not in actual_game_stats['player_position_abbreviation'].unique()
        assert 'Goalie' not in actual_game_stats['player_position_name'].unique()
        assert 'side' in actual_game_stats

    def test_parse_player_data(self):
        # Set Up
        player_data = [json.load(open('resources/test_player.json'))]
        # Test
        actual_player_data_df = self.crawler.parse_player_data(player_data)

        # Verify
        for col in actual_player_data_df:
            if col != 'side':
                assert col.split('_')[0] == 'player'
            assert '.' not in col

    def test_parse_crawl_args(self):
        # Set Up
        start_date = '2020-08-04'
        end_date = '2020-08-05'

        expected_format = '%Y-%m-%d'
        expected_start_date = datetime.strptime(start_date, expected_format)
        expected_end_date = datetime.strptime(end_date, expected_format)

        # Test invalid types
        with self.assertRaises(expected_exception=TypeError):
            parse_crawl_args(start_date=2020, end_date=2022)
        # Test invalid start_date
        with self.assertRaises(expected_exception=ValueError):
            parse_crawl_args(start_date='2020-ab-12', end_date='2022-01-01')
        # Test invalid end_date
        with self.assertRaises(expected_exception=ValueError):
            parse_crawl_args(start_date='2020-01-12', end_date='2022-ab-01')
        # Test start_date > end_date
        with self.assertRaises(expected_exception=ValueError):
            parse_crawl_args(start_date='2022-01-12', end_date='2020-01-01')
        # Test valid input
        actual_start_date, actual_end_date = parse_crawl_args(start_date, end_date)
        # Verify
        assert (actual_start_date, actual_end_date) == (expected_start_date, expected_end_date)

