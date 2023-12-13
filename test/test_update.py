from unittest import TestCase, main
import sqlite3 as sql3

from src.av_update import *
from src.av_warehouse import Warehouse
from src.av_api import AlphaVantage, decode_price_data, decode_earnings_data, decode_fundamentals, decode_company_data

def mock_api(test_case: TestCase) -> AlphaVantage:
    price_response = open('test/samples/WEEKLY_ADJUSTED_IBM.csv').read()
    earnings_response = open('test/samples/EARNINGS_IBM.json').read()
    cash_flow_response = open('test/samples/CASH_FLOW_IBM.json').read()
    income_statement_response = open('test/samples/INCOME_STATEMENT_IBM.json').read()
    balance_sheet_response = open('test/samples/BALANCE_SHEET_IBM.json').read()
    overview_response = open('test/samples/OVERVIEW_IBM.json').read()
    def fake_request(url: str) -> str: 
        if not 'symbol=IBM' in url: raise Exception(f'Unexpected URL: {url}')
        if 'WEEKLY_ADJUSTED' in url:
            return price_response
        elif 'EARNINGS' in url:
            return earnings_response
        elif 'CASH_FLOW' in url:
            return cash_flow_response
        elif 'INCOME_STATEMENT' in url:
            return income_statement_response
        elif 'BALANCE_SHEET' in url:
            return balance_sheet_response
        elif 'OVERVIEW' in url:
            return overview_response
        else:
            raise Exception(f'Unexpected URL: {url}')
    return AlphaVantage('TEST_KEY', fake_request)

class EmptyWareHouse(TestCase):
    def setUp(self) -> None:
        self.api = mock_api(self)

        migration = open('migration/migrate.sql').read()
        memory_db = sql3.connect(':memory:')
        memory_db.executescript(migration)
        self.warehouse = Warehouse(memory_db)

    def test_should_fill_prices(self):
        update_price_data(self.api, self.warehouse, ['IBM'])
        first_row = (
            'IBM', '2023-12-01', '154.9900', '160.5900', '154.7500', 
            '160.5500', '160.5500', '21900644', '0.0000' 
        )
        self.assertEqual(first_row, self.warehouse.list_rows('price_data')[0])

    def test_should_fill_earnings(self):
        update_earnings_data(self.api, self.warehouse, ['IBM'])
        first_row = (
            "IBM", "2023-09-30", "2023-10-25", "2.2", "2.13", "0.07", "3.2864" 
        )
        self.assertEqual(first_row, self.warehouse.list_rows('earnings_data')[0])

    def test_should_fill_cashflow(self):
        update_cashflow_data(self.api, self.warehouse, ['IBM'])
        first_row = (
            "IBM", "2023-09-30", "USD", "3056000000", "None", "None",
            "None", "None", "1093000000", "281000000", "None", "None", 
            "1714000000", "-1953000000", "-3132000000", "9000000",
            "None", "None", "None", "1515000000", "1515000000", "None", 
            "None", "154000000", "None", "-98000000", "None", "None", 
            "None", "1704000000"
        )
        self.assertIn(first_row, self.warehouse.list_rows('cashflow_data'))
    
    def test_should_fill_income_statement(self):
        update_income_statement(self.api, self.warehouse, ['IBM'])
        first_row = (
            "IBM", "2023-09-30", "USD", "8023000000", "14752000000",
            "6729000000", "42000000", "1994000000", "4458000000", "1685000000",
            "6029000000", "None", "-412000000", "156000000", "412000000", "265000000", 
            "97000000", "521000000", "572000000", "1863000000",
            "159000000", "412000000", "1714000000", "2105000000",
            "2275000000", "2847000000", "1704000000"
        )
        self.assertIn(first_row, self.warehouse.list_rows('income_statement'))
    
    def test_should_fill_balance_sheet(self):
        update_balance_sheet(self.api, self.warehouse, ['IBM'])
        first_row = (
            "IBM", "2023-09-30", "USD", "129321000000", "27705000000", 
            "7257000000", "7257000000", "1399000000", "6039000000", "100035000000",
            "5369000000", "12848000000", "70874000000", "11278000000", "59596000000",
            "None", "None", "3721000000", "2581000000", "None", "106165000000",
            "30606000000", "3342000000", "15002000000", "12814000000",
            "6414000000", "75560000000", "3283000000", "50664000000",
            "6400000000", "48828000000", "84575000000", "8126000000",
            "12081000000", "23081000000", "169640000000", "149506000000", 
            "59313000000", "912800000"
        )
        self.assertIn(first_row, self.warehouse.list_rows('balance_sheet'))

    def test_should_fill_companies(self):
        update_company_data(self.api, self.warehouse, ['IBM'])
        first_row = (
            "IBM", "Common Stock", "International Business Machines",
            "International Business Machines Corporation (IBM) is an American multinational technology company headquartered in Armonk, New York, with operations in over 170 countries. The company began in 1911, founded in Endicott, New York, as the Computing-Tabulating-Recording Company (CTR) and was renamed International Business Machines in 1924. IBM is incorporated in New York. IBM produces and sells computer hardware, middleware and software, and provides hosting and consulting services in areas ranging from mainframe computers to nanotechnology. IBM is also a major research organization, holding the record for most annual U.S. patents generated by a business (as of 2020) for 28 consecutive years. Inventions by IBM include the automated teller machine (ATM), the floppy disk, the hard disk drive, the magnetic stripe card, the relational database, the SQL programming language, the UPC barcode, and dynamic random-access memory (DRAM). The IBM mainframe, exemplified by the System/360, was the dominant computing platform during the 1960s and 1970s.",
            "51143", "NYSE", "USD", "USA", "TECHNOLOGY",
            "COMPUTER & OFFICE EQUIPMENT",  "1 NEW ORCHARD ROAD, ARMONK, NY, US", 
            "December", "2023-09-30", "146601247000", "13663000000", "20.72", "0.429",
            "25.28", "6.62",  "0.0414", "7.75", "67.3", "0.113", "0.145",
            "0.0455", "0.328", "61170999000", "32688000000",
            "7.75", "0.126", "0.046", "145.79", "20.72", "16.08", "2.397", 
            "6.35", "3.174", "13.73", "0.772", "162.79", "117.38", 
            "145.97", "136.88", "913119000", "2023-12-09", "2023-11-09"
        )
        self.assertEqual([first_row], self.warehouse.list_rows('company_data'))

class NonEmptyWarehouse(TestCase):
    def setUp(self) -> None:
        self.api = mock_api(self)

        migration = open('migration/migrate.sql').read()
        memory_db = sql3.connect(':memory:')
        memory_db.executescript(migration)
        self.warehouse = Warehouse(memory_db)

        prices_response = self.api.get_weekly_adjusted('IBM')
        self.new_prices = [('IBM',) + row for row in decode_price_data(prices_response)]
        old_prices = self.new_prices[:-12]
        self.warehouse.extend_table('price_data', old_prices)

        earnings_response = self.api.get_earnings('IBM')
        self.new_earnings = [('IBM',) + row for row in decode_earnings_data(earnings_response)]
        old_earnings = self.new_earnings[:-4]
        self.warehouse.extend_table('earnings_data', old_earnings)

        cash_flow_response = self.api.get_cash_flow('IBM')
        self.new_cash_flow = [('IBM',) + row for row in decode_fundamentals(cash_flow_response)]
        old_cash_flow = self.new_cash_flow[:-4]
        self.warehouse.extend_table('cashflow_data', old_cash_flow)

        income_statement_response = self.api.get_income_statement('IBM')
        self.new_income_statement = [('IBM',) + row for row in decode_fundamentals(income_statement_response)]
        old_income_statement = self.new_income_statement[:-4]
        self.warehouse.extend_table('income_statement', old_income_statement)

        balance_sheet_response = self.api.get_balance_sheet('IBM')
        self.new_balance_sheet = [('IBM',) + row for row in decode_fundamentals(balance_sheet_response)]
        old_balance_sheet = self.new_balance_sheet[:-4]
        self.warehouse.extend_table('balance_sheet', old_balance_sheet)

        overview_response = self.api.get_company_overview('IBM')
        self.new_overview = decode_company_data(overview_response)
        row_width = len(self.new_overview[0])
        old_overview = [('IBM',) + ('DID_NOT_UPDATE',) * (row_width - 1)]
        self.warehouse.update_table('company_data', old_overview)

    def test_should_update_prices(self):
        update_price_data(self.api, self.warehouse, ['IBM'])
        self.assertEqual(self.new_prices, self.warehouse.list_rows('price_data'))

    def test_should_update_earnings(self):
        update_earnings_data(self.api, self.warehouse, ['IBM'])
        self.assertEqual(self.new_earnings, self.warehouse.list_rows('earnings_data'))

    def test_should_update_cashflow(self):
        update_cashflow_data(self.api, self.warehouse, ['IBM'])
        self.assertEqual(self.new_cash_flow, self.warehouse.list_rows('cashflow_data'))
    
    def test_should_update_income_statement(self):
        update_income_statement(self.api, self.warehouse, ['IBM'])
        self.assertEqual(self.new_income_statement, self.warehouse.list_rows('income_statement'))
    
    def test_should_update_balance_sheet(self):
        update_balance_sheet(self.api, self.warehouse, ['IBM'])
        self.assertEqual(self.new_balance_sheet, self.warehouse.list_rows('balance_sheet'))

    def test_should_update_companies(self):
        update_company_data(self.api, self.warehouse, ['IBM'])
        self.assertEqual(self.new_overview, self.warehouse.list_rows('company_data'))



if __name__ == '__main__':
    main()