import av_api as av
import av_warehouse as wh
from datetime import date, timedelta
import os, sys

ALPHA_VANTAGE_KEY = os.environ['ALPHA_VANTAGE_KEY']
ETL_WAREHOUSE_PATH = os.environ['ETL_WAREHOUSE_PATH']

def run(api_key = ALPHA_VANTAGE_KEY, warehouse_path = ETL_WAREHOUSE_PATH, *args):
    api = av.AlphaVantage(api_key)
    warehouse = wh.Warehouse(db_url=warehouse_path)
    ticks = [key[0] for key in warehouse.list_keys('company_data')]
    shell_args = args or {arg for arg in sys.argv}

    if '--update-overview' in shell_args: 
        update_company_data(api, warehouse, ticks)
    if '--update-prices'in shell_args:
        update_price_data(api, warehouse, ticks)
    update_earnings_data(api, warehouse, ticks)
    update_income_statement(api, warehouse, ticks)
    update_cashflow_data(api, warehouse, ticks)
    update_balance_sheet(api, warehouse, ticks)

def update_price_data(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str], limit_date: date = None) -> None:
    table = 'price_data'
    limit_date = limit_date or date.today()
    update_period = timedelta(days=7)
    for tick in ticks:
        last_update = warehouse.latest_timestamp(table, tick)
        next_update = last_update + update_period
        if limit_date < next_update:
            continue
        data = api.get_weekly_adjusted(tick)
        data = av.decode_price_data(data, tick) 
        warehouse.extend_table(table, data)

def update_earnings_data(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str], limit_date: date = None) -> None:
    table = 'earnings_data'
    limit_date = limit_date or date.today()
    update_period = timedelta(days=90)
    for tick in ticks:
        last_update = warehouse.latest_timestamp(table, tick)
        next_update = last_update + update_period
        if limit_date < next_update:
            continue
        data = api.get_earnings(tick)
        data = av.decode_earnings_data(data, tick)
        warehouse.extend_table(table, data)

def update_cashflow_data(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str], limit_date: date = None) -> None:
    table = 'cashflow_data'
    limit_date = limit_date or date.today()
    update_period = timedelta(days=90)
    for tick in ticks:
        last_update = warehouse.latest_timestamp(table, tick)
        next_update = last_update + update_period
        if limit_date < next_update:
            continue
        data = api.get_cash_flow(tick)
        data = av.decode_fundamentals(data, tick)
        warehouse.extend_table(table, data)

def update_income_statement(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str], limit_date: date = None) -> None:
    table = 'income_statement'
    limit_date = limit_date or date.today()
    update_period = timedelta(days=90)
    for tick in ticks:
        last_update = warehouse.latest_timestamp(table, tick)
        next_update = last_update + update_period
        if limit_date < next_update:
            continue
        data = api.get_income_statement(tick)
        data = av.decode_fundamentals(data, tick)
        warehouse.extend_table(table, data)

def update_balance_sheet(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str], limit_date: date = None) -> None:
    table = 'balance_sheet'
    limit_date = limit_date or date.today()
    update_period = timedelta(days=90)
    for tick in ticks:
        last_update = warehouse.latest_timestamp(table, tick)
        next_update = last_update + update_period
        if limit_date < next_update:
            continue
        data = api.get_balance_sheet(tick)
        data = av.decode_fundamentals(data, tick)
        warehouse.extend_table(table, data)

def update_company_data(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str]) -> None:
    table = 'company_data'
    for tick in ticks:
        data = api.get_company_overview(tick)
        data = av.decode_company_data(data) 
        warehouse.update_table(table, data)

if __name__ == '__main__':
    run()