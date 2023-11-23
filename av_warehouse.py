
#from av_load import load_data
#from os.path import exists
import pandas as pd
#import json
#from io import StringIO

DATA_DIR = './data/alpha_vantage'

'''
def store_data(data: pd.DataFrame, table: str, tick: str):
    path = f'{DATA_DIR}/{table}/{tick}.ftr'
    return data.to_feather(path)

def table_exists(table: str, tick: str) -> bool: 
    path = f'{DATA_DIR}/{table}/{tick}.ftr'
    return exists(path)

def get_tick_list(list_path):
    with open(list_path) as file:
        ticks = []
        for line in file.readlines():
            ticks.append(line.rstrip())
        return ticks
'''
class Warehouse:
    def __init__(self) -> None:
        #self._api = AlphaVantage(api_key)
        self._data_dir = DATA_DIR

    def extend(self, table: str, data: pd.DataFrame):
        raise Exception()
    
    def list_keys(self, table: str) -> list:
        raise Exception()
    
    def extend_keys(self, table: str, keys: list):
        raise Exception()
    
    def remove_keys(self, table: str, keys: list):
        raise Exception()
    
    def clear_table(self, table: str):
        raise Exception()
    
    '''
    def init_price_data(self, tick):
        table = 'price_data'
        if not table_exists(table, tick):
            query = self._api.daily_adjusted_query(tick, data_type='csv', output_size='full')
            data = fetch_data(query)
            data = decode_price_data(data)
            store_data(data, table, tick)

    def init_overview_data(self, tick):
        table = 'company_data'
        if not table_exists(table, tick):
            query = self._api.company_overview_query(tick)
            data = fetch_data(query)
            data = decode_company_data(data)
            store_data(data, table, tick)

    def init_income_statement(self, tick):
        table = 'income_statements'
        if not table_exists(table, tick):
            query = self._api.income_statement_query(tick)
            data = fetch_data(query)
            data = decode_fundamentals(data)
            store_data(data, table, tick)

    def init_balance_sheet(self, tick):
        table = 'balance_sheets'
        if not table_exists(table, tick):
            query = self._api.balance_sheet_query(tick)
            data = fetch_data(query)
            data = decode_fundamentals(data)
            store_data(data, table, tick)

    def init_cashflow_data(self, tick): 
        table = 'cashflow_data'
        if not table_exists(table, tick):
            query = self._api.cash_flow_query(tick)
            data = fetch_data(query)
            data = decode_fundamentals(data)
            store_data(data, table, tick)

    def init_earnings_data(self, tick): 
        table = 'earnings_data'
        if not table_exists(table, tick):
            query = self._api.earnings_query(tick)
            data = fetch_data(query)
            data = decode_earnings_data(data)
            store_data(data, table, tick)

    def update_price_data(self, tick):
        table = 'price_data'
        if table_exists(table, tick):
            current_data = load_data(table, tick)
            current_data.set_index('timestamp', inplace=True)

            query = self._api.daily_adjusted_query(tick, data_type='csv', output_size='full')
            new_data = fetch_data(query)
            new_data = decode_price_data(new_data)
            new_data.set_index('timestamp', inplace=True)

            final_data = pd.concat([current_data, new_data], axis='index')
            final_data.drop_duplicates(inplace=True)
            final_data.sort_index(ascending=False, inplace=True)

            store_data(final_data.reset_index(), table, tick)

    def update_earnings_data(self, tick): 
        table = 'earnings_data'
        if table_exists(table, tick):
            current_data = load_data(table, tick)
            current_data.set_index('timestamp', inplace=True)

            query = self._api.earnings_query(tick)
            data = fetch_data(query)
            store_data(data, table, tick)

            new_data = fetch_data(query)
            new_data = decode_earnings_data(new_data)
            new_data.set_index('timestamp', inplace=True)

    def update_company_data(self, tick):
        table = 'company_data'
        #overview_path = f'{DATA_FOLDER}/company_data/{tick}.json'
        if table_exists(table, tick):
            query = self._api.company_overview_query(tick)
            data = fetch_data(query)
            store_bytes(data, overview_path)

    def update_income_statement(self, tick):
        table = 'income_statements'
        #income_path = f'{DATA_FOLDER}/income_statements/{tick}.json'
        if table_exists(table, tick):
            query = self._api.income_statement_query(tick)
            data = fetch_data(query)
            store_bytes(data, income_path)

    def update_balance_sheet(self, tick):
        table = 'balance_sheets'
        #balance_path = f'{DATA_FOLDER}/balance_sheets/{tick}.json'
        if table_exists(table, tick):
            query = self._api.balance_sheet_query(tick)
            data = fetch_data(query)
            store_bytes(data, balance_path)

    def update_cashflow_data(self, tick):
        table = 'cashflow_data'
        #cashflow_path = f'{DATA_FOLDER}/cashflow_data/{tick}.json'
        if table_exists(table, tick):
            query = self._api.cash_flow_query(tick)
            data = fetch_data(query)
            store_bytes(data, cashflow_path)

    '''