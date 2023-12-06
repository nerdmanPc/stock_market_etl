from unittest import TestCase, main
import sqlite3 as sql3

from src.av_update import update_price_data, update_earnings_data, update_company_data
from src.av_warehouse import Warehouse
from src.av_api import AlphaVantage

class EmptyWareHouse(TestCase):
    def setUp(self) -> None:
        price_response = open('test/samples/WEEKLY_ADJUSTED_IBM.csv').read()
        earnings_response = open('test/samples/EARNINGS_IBM.json').read()
        overview_response = open('test/samples/OVERVIEW_IBM.json').read()
        def fake_request(url: str) -> str: 
            if 'WEEKLY_ADJUSTED' in url:
                return price_response
            elif 'EARNINGS' in url:
                return earnings_response
            elif 'OVERVIEW' in url:
                return overview_response
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

    def test_should_fill_companies_table(self):
        update_company_data(self.api, self.warehouse, 'IBM')
        self.assertEqual([(
            "IBM",
            "Common Stock",
            "International Business Machines",
            "International Business Machines Corporation (IBM) is an American multinational technology company headquartered in Armonk, New York, with operations in over 170 countries. The company began in 1911, founded in Endicott, New York, as the Computing-Tabulating-Recording Company (CTR) and was renamed International Business Machines in 1924. IBM is incorporated in New York. IBM produces and sells computer hardware, middleware and software, and provides hosting and consulting services in areas ranging from mainframe computers to nanotechnology. IBM is also a major research organization, holding the record for most annual U.S. patents generated by a business (as of 2020) for 28 consecutive years. Inventions by IBM include the automated teller machine (ATM), the floppy disk, the hard disk drive, the magnetic stripe card, the relational database, the SQL programming language, the UPC barcode, and dynamic random-access memory (DRAM). The IBM mainframe, exemplified by the System/360, was the dominant computing platform during the 1960s and 1970s.",
            "51143",
            "NYSE",
            "USD",
            "USA",
            "TECHNOLOGY",
            "COMPUTER & OFFICE EQUIPMENT",
            "1 NEW ORCHARD ROAD, ARMONK, NY, US",
            "December",
            "2023-09-30",
            "146601247000",
            "13663000000",
            "20.72",
            "0.429",
            "25.28",
            "6.62",
            "0.0414",
            "7.75",
            "67.3",
            "0.113",
            "0.145",
            "0.0455",
            "0.328",
            "61170999000",
            "32688000000",
            "7.75",
            "0.126",
            "0.046",
            "145.79",
            "20.72",
            "16.08",
            "2.397",
            "6.35",
            "3.174",
            "13.73",
            "0.772",
            "162.79",
            "117.38",
            "145.97",
            "136.88",
            "913119000",
            "2023-12-09",
            "2023-11-09"
        )], self.warehouse.list_rows('company_data'))

if __name__ == '__main__':
    main()