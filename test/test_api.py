import unittest as test
import src.av_api as api
import csv

class PriceDataCsv(test.TestCase):

    def setUp(self) -> None:
        super().setUp()
        with open('test/samples/price_query.csv', 'r') as file:
            self.input = file.read()
            self.assertTrue(isinstance(self.input, str))

    def test_sholud_convert_to_list_of_tuples(self):
        result = api.decode_price_data(self.input)
        self.assertTrue(isinstance(result, list))
        self.assertIn( (
            '2023-11-29',
            '156.15',
            '157.51',
            '156.02',
            '156.41',
            '156.41',
            '3568887',
            '0.0000',
            '1.0'
        ), result)
    
    def tearDown(self) -> None:
        return super().tearDown()
    
class EarningsDataJson(test.TestCase):

    def setUp(self) -> None:
        super().setUp()
        with open('test/samples/earnings_query.json', 'r') as file:
            self.input = file.read()
    
    def test_sholud_convert_to_list_of_tuples(self):
        result = api.decode_earnings_data(self.input)
        self.assertTrue(isinstance(result, list))
        self.assertIn( (
            "2023-09-30",
            "2023-10-25",
            "2.2",
            "2.13",
            "0.07",
            "3.2864"
        ), result)
    
    def tearDown(self) -> None:
        return super().tearDown()

'''
class FundamentalsJson(test.TestCase):

    def setUp(self) -> None:
        return super().setUp()
    
    def tearDown(self) -> None:
        return super().tearDown()
'''