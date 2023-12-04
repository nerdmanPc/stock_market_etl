from unittest import TestCase
import sqlite3 as sql3

from src.av_update import update_price_data, update_earnings_data
from src.av_warehouse import Warehouse
from src.av_api import AlphaVantage

class EmptyWareHouse(TestCase):
    def setUp(self) -> None:
        price_response = open('test/samples/WEEKLY_ADJUSTED_IBM.csv').read()
        earnings_response = open('test/samples/EARNINGS_IBM.json').read()
        def fake_request(url: str) -> str: 
            if 'WEEKLY_ADJUSTED' in url:
                return price_response
            elif 'EARNINGS' in url:
                return earnings_response
            else:
                self.assertTrue(False, f'Unexpected URL: {url}')
        self.api = AlphaVantage('TEST_KEY', fake_request)

        migration = open('migration/migrate.sql').read()
        memory_db = sql3.connect(':memory:')
        memory_db.executescript(migration)
        self.warehouse = Warehouse(memory_db)

    def test_should_fill_prices_table(self):
        update_price_data(self.api, self.warehouse, 'IBM')
        self.assertEqual((
            'IBM', 
            '2023-12-01', 
            '154.9900', 
            '160.5900', 
            '154.7500', 
            '160.5500', 
            '160.5500', 
            '21900644', 
            '0.0000'
        ), self.warehouse.list_rows('price_data')[0])

    def test_should_fill_earnings_table(self):
        update_earnings_data(self.api, self.warehouse, 'IBM')
        self.assertEqual((
            "IBM",
            "2023-09-30",
            "2023-10-25",
            "2.2",
            "2.13",
            "0.07",
            "3.2864"
        ), self.warehouse.list_rows('earnings_data')[0])

