
import requests as http
from time import sleep
from io import StringIO
import pandas as pd
import json

AV_ENDPOINT = 'https://alphavantage.co/query'

class AlphaVantage:

    def __init__(self, api_key=None) -> None:
        self._api_key = api_key or get_api_key()

    def search_symbol(self, keywords, data_type='json'):
        query = self.symbol_search_query(keywords, data_type)
        return fetch_data(query)

    def symbol_search_query(self, keywords, data_type='json'):
        params = params = f'function=SYMBOL_SEARCH&apikey={self._api_key}&keywords={keywords}'
        if data_type == 'csv' or data_type == 'json':
            params += f'&datatype={data_type}'
        else:
            raise Exception(f'Invalid data type: {data_type}')
        return f'{AV_ENDPOINT}?{params}'
    
    def get_intraday(self, symbol, interval_mins, output_size='compact', data_type='csv'):
        query = self.intraday_query_query(symbol, interval_mins, output_size, data_type)
        return fetch_data(query)

    def intraday_query(self, symbol, interval_mins, output_size='compact', data_type='csv'):
        params = f'function=TIME_SERIES_INTRADAY&apikey={self._api_key}&symbol={symbol}'
        if interval_mins in [1, 5, 30, 60, '1', '5', '30', '60']:
            params += f'&interval={interval_mins}min'
        else:
            raise Exception(f'Invalid interval: {interval_mins}')
        if output_size == 'compact' or output_size == 'full':
            params += f'&outputsize={output_size}'
        else:
            raise Exception(f'Invalid output size: {output_size}')
        if data_type == 'csv' or data_type == 'json':
            params += f'&datatype={data_type}'
        else:
            raise Exception(f'Invalid data type: {data_type}')
        return f'{AV_ENDPOINT}?{params}'
    
    def get_daily_adjusted(self, symbol, output_size='compact', data_type='csv'):
        query = self.daily_adjusted_query(symbol, output_size, data_type)
        return fetch_data(query)

    def daily_adjusted_query(self, symbol, output_size='compact', data_type='csv'):
        params = f'function=TIME_SERIES_DAILY_ADJUSTED&apikey={self._api_key}&symbol={symbol}'
        if output_size == 'compact' or output_size == 'full':
            params += f'&outputsize={output_size}'
        else:
            raise Exception(f'Invalid output size: {output_size}')
        if data_type == 'csv' or data_type == 'json':
            params += f'&datatype={data_type}'
        else:
            raise Exception(f'Invalid data type: {data_type}')
        return f'{AV_ENDPOINT}?{params}'
    
    def get_company_overview(self, symbol):
        query = self.company_overview_query(symbol)
        return fetch_data(query)

    def company_overview_query(self, symbol):
        params = f'function=OVERVIEW&apikey={self._api_key}&symbol={symbol}'
        return f'{AV_ENDPOINT}?{params}'
    
    def get_income_statement(self, symbol):
        query = self.income_statement_query(symbol)
        return fetch_data(query)

    def income_statement_query(self, symbol):
        params = f'function=INCOME_STATEMENT&apikey={self._api_key}&symbol={symbol}'
        return f'{AV_ENDPOINT}?{params}'

    def get_balance_sheet(self, symbol):
        query = self.balance_sheet_query(symbol)
        return fetch_data(query)

    def balance_sheet_query(self, symbol):
        params = f'function=BALANCE_SHEET&apikey={self._api_key}&symbol={symbol}'
        return f'{AV_ENDPOINT}?{params}'
    
    def get_cash_flow(self, symbol):
        query = self.cash_flow_query(symbol)
        return fetch_data(query)

    def cash_flow_query(self, symbol):
        params = f'function=CASH_FLOW&apikey={self._api_key}&symbol={symbol}'
        return f'{AV_ENDPOINT}?{params}'
    
    def get_earnings(self, symbol):
        query = self.earnings_query(symbol)
        return fetch_data(query)

    def earnings_query(self, symbol):
        params = f'function=EARNINGS&apikey={self._api_key}&symbol={symbol}'
        return f'{AV_ENDPOINT}?{params}'

def get_api_key():
    with open('api-key.txt') as file:
        return file.read()

def fetch_data(url: str) -> str:
    sleep(12)
    req = http.get(url)
    if req.status_code != 200:
        raise Exception(f'Request error: {req.status_code} - {req.content}')
    data = str(req.content, 'utf-8')
    check_api_error(data, url)
    return data

def check_api_error(data: str, url: str):
    try: # Happy path is when there is no 'Error Message'
        err_msg = json.loads(data)['Error Message']
        raise Exception(f'API error!\nMessage: {err_msg}\nURL: {url}')
    except:
        pass

def decode_price_data(data: str) -> pd.DataFrame:
    return pd.read_csv(StringIO(data))

def decode_earnings_data(data: str) -> pd.DataFrame:
    data = json.loads(data)
    if 'quarterlyEarnings' in data:
        data = data['quarterlyEarnings']
    else:
        data = data['annualEarnings']
    return pd.DataFrame(data)

def decode_fundamentals(data: str) -> pd.DataFrame:
    data = json.loads(data)
    if 'quarterlyEarnings' in data:
        data = data['quarterlyReports']
    else:
        data = data['annualReports']
    return pd.DataFrame(data)

def decode_company_data(data: str) -> pd.DataFrame:
    data = json.loads(data)
    return pd.DataFrame([data])