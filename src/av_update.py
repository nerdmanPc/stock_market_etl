import src.av_api as av
import src.av_warehouse as wh

WAREHOUSE_DIRECTORY = 'data/'

def run():
    api = av.AlphaVantage()
    warehouse = wh.Warehouse(wh.connect(f'{WAREHOUSE_DIRECTORY}warehouse.db'))
    ticks = warehouse.list_keys('companies_data')

    for (tick,) in ticks:
        update_company_data(api, warehouse, tick)

    for (tick,) in ticks:
        update_earnings_data(api, warehouse, tick)

    for (tick,) in ticks:
        update_cashflow_data(api, warehouse, tick)

    for (tick,) in ticks:
        update_balance_sheet(api, warehouse, tick)
    
    for (tick,) in ticks:
        update_income_statement(api, warehouse, tick)

    for (tick,) in ticks:
        update_price_data(api, warehouse, tick)

def update_price_data(api: av.AlphaVantage, warehouse: wh.Warehouse, tick: str) -> None:
    table = 'price_data'
    data = api.get_weekly_adjusted(tick)
    data = av.decode_price_data(data) 
    data = [(tick,) + row for row in data]
    warehouse.extend_table(table, data)

def update_earnings_data(api: av.AlphaVantage, warehouse: wh.Warehouse, tick: str) -> None:
    table = 'earnings_data'
    data = api.get_earnings(tick)
    data = av.decode_earnings_data(data)
    data = [(tick,) + row for row in data]
    warehouse.extend_table(table, data)

def update_cashflow_data(api: av.AlphaVantage, warehouse: wh.Warehouse, tick: str) -> None:
    table = 'cashflow_data'
    data = api.get_cash_flow(tick)
    data = av.decode_fundamentals(data)
    data = [(tick,) + row for row in data]
    warehouse.extend_table(table, data)

def update_income_statement(api: av.AlphaVantage, warehouse: wh.Warehouse, tick: str) -> None:
    table = 'income_statement'
    data = api.get_income_statement(tick)
    data = av.decode_fundamentals(data)
    data = [(tick,) + row for row in data]
    warehouse.extend_table(table, data)

def update_balance_sheet(api: av.AlphaVantage, warehouse: wh.Warehouse, tick: str) -> None:
    table = 'balance_sheet'
    data = api.get_balance_sheet(tick)
    data = av.decode_fundamentals(data)
    data = [(tick,) + row for row in data]
    warehouse.extend_table(table, data)

def update_company_data(api: av.AlphaVantage, warehouse: wh.Warehouse, tick: str) -> None:
    table = 'company_data'
    data = api.get_company_overview(tick)
    data = av.decode_company_data(data) 
    warehouse.update_table(table, data)

if __name__ == '__main__':
    run()