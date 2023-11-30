import src.av_warehouse as wh

warehouse = wh.Warehouse()

tick_list = ''
with open('tick_list.txt', 'r') as file:
    try: 
        tick_list = file.read()
    except:
        pass

def list_companies():
    ticks = warehouse.list_keys('company_data')
    for tick in ticks:
        print(tick)

def add_companies(ticks: str | list[str]):
    if type(ticks) is str:
        ticks = ticks.split()
    warehouse.extend_keys('company_data', ticks)

def remove_companies(ticks: str | list[str]):
    if type(ticks) is str:
        ticks = ticks.split()
    warehouse.remove_keys('company_data', ticks)

def clear_companies():
    warehouse.clear_table('company_data')