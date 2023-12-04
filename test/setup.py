import sqlite3 as sql3
from src.av_warehouse import Warehouse

class WarehouseSetup:
    def __init__(self) -> None:

        self.table = 'test_table'
        self.empty_table = 'empty_table'

        self.old_data = [('AAPL', 1), ('NVDA', 2), ('IBM', 3)]
        self.new_data = [('TSLA', 4), ('GOOG', 5), ('AMD', 6)]

        test_db = sql3.connect(':memory:')
        test_db.execute(f'CREATE TABLE {self.table} (key PRIMARY KEY, value)')
        test_db.execute(f'CREATE TABLE {self.empty_table} (key PRIMARY KEY, value)')

        test_db.executemany(f'INSERT INTO {self.table} VALUES (?, ?)', self.old_data)
        test_db.commit()

        self.warehouse = self.warehouse = Warehouse(test_db)