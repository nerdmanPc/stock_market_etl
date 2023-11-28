import src.av_api as av
import src.av_warehouse as wh

def run():
    api = av.AlphaVantage()
    warehouse = wh.Warehouse()
    ticks = warehouse.list_keys('company_data')

    for tick in ticks:
        #warehouse.init_price_data(tick)
        #warehouse.init_overview_data(tick)
        #warehouse.init_income_statement(tick)
        #warehouse.init_balance_sheet(tick)
        #warehouse.init_cashflow_data(tick)
        #warehouse.init_earnings_data(tick)

        #warehouse.update_overview_data(tick)
        #warehouse.update_income_statement(tick)
        #warehouse.update_balance_sheet(tick)
        #warehouse.update_cashflow_data(tick)
        update_company_data(api, warehouse, tick)
        update_earnings_data(api, warehouse, tick)
        update_price_data(api, warehouse, tick)

def update_price_data(api: av.AlphaVantage, warehouse: wh.Warehouse, tick: str) -> None:
    table = 'price_data'
    data = api.get_daily_adjusted(tick)
    data = av.decode_price_data(data).set_index('timestamp')
    warehouse.extend_table(table, data)

def update_earnings_data(api: av.AlphaVantage, warehouse: wh.Warehouse, tick: str) -> None:
    table = 'earnings_data'
    data = api.get_earnings(tick)
    data = av.decode_earnings_data(data).set_index('timestamp')
    warehouse.extend_table(table, data)

def update_company_data(api: av.AlphaVantage, warehouse: wh.Warehouse, tick: str) -> None:
    table = 'company_data'
    data = api.get_company_overview(tick)
    data = av.decode_company_data(data)
    warehouse.extend_table(table, data)