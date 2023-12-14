import src.av_api as av
import src.av_warehouse as wh

WAREHOUSE_PATH = 'data/warehouse.db'

def run():
    api = av.AlphaVantage()
    warehouse = wh.Warehouse(wh.sqlite_connection(WAREHOUSE_PATH))
    ticks = [key[0] for key in warehouse.list_keys('companies_data')]

    update_company_data(api, warehouse, ticks)
    update_earnings_data(api, warehouse, ticks)
    update_cashflow_data(api, warehouse, ticks)
    update_balance_sheet(api, warehouse, ticks)
    update_income_statement(api, warehouse, ticks)
    update_price_data(api, warehouse, ticks)

def update_price_data(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str]) -> None:
    table = 'price_data'
    for tick in ticks:
        data = api.get_weekly_adjusted(tick)
        data = av.decode_price_data(data) 
        data = [(tick,) + row for row in data]
        warehouse.extend_table(table, data)

def update_earnings_data(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str]) -> None:
    table = 'earnings_data'
    for tick in ticks:
        data = api.get_earnings(tick)
        data = av.decode_earnings_data(data)
        data = [(tick,) + row for row in data]
        warehouse.extend_table(table, data)

def update_cashflow_data(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str]) -> None:
    table = 'cashflow_data'
    for tick in ticks:
        data = api.get_cash_flow(tick)
        data = av.decode_fundamentals(data)
        data = [(tick,) + row for row in data]
        warehouse.extend_table(table, data)

def update_income_statement(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str]) -> None:
    table = 'income_statement'
    for tick in ticks:
        data = api.get_income_statement(tick)
        data = av.decode_fundamentals(data)
        data = [(tick,) + row for row in data]
        warehouse.extend_table(table, data)

def update_balance_sheet(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str]) -> None:
    table = 'balance_sheet'
    for tick in ticks:
        data = api.get_balance_sheet(tick)
        data = av.decode_fundamentals(data)
        data = [(tick,) + row for row in data]
        warehouse.extend_table(table, data)

def update_company_data(api: av.AlphaVantage, warehouse: wh.Warehouse, ticks: list[str]) -> None:
    table = 'company_data'
    for tick in ticks:
        data = api.get_company_overview(tick)
        data = av.decode_company_data(data) 
        warehouse.update_table(table, data)

if __name__ == '__main__':
    run()