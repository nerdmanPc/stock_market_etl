import pandas as pd
import sqlite3 as sql3

def connect(db_path):
    return sql3.connect(db_path)
class Warehouse:
    def __init__(self, db_conn) -> None:
        self.db_conn = db_conn

    def extend_table(self, table: str, data: list):
        table_info_query = f"PRAGMA table_info({table})"
        columns = self.db_conn.execute(table_info_query).fetchall()
        #dbg = { col[1]: value for col, value in zip(columns, data[0]) }
        placeholders = ','.join(['?'] * len(columns))
        extend_query = f'INSERT OR IGNORE INTO {table} VALUES ({placeholders})'
        self.db_conn.executemany(extend_query, data)
        self.db_conn.commit()
    
    def list_keys(self, table: str) -> list:
        table_info_query = f"PRAGMA table_info({table})"
        columns = self.db_conn.execute(table_info_query).fetchall()

        is_primary_key = lambda x: x[-1] != 0
        get_primary_key = lambda x: x[-1]
        get_name = lambda x: x[1]
        
        key_cols = map(get_name, sorted(filter(is_primary_key, columns), key=get_primary_key))
        key_cols = ','.join(key_cols)
        list_keys_query = f'SELECT {key_cols} FROM {table}'
        cursor = self.db_conn.execute(list_keys_query)
        return cursor.fetchall()
    
    def list_rows(self, table: str) -> list:
        list_rows_query = f'SELECT * FROM {table}'
        cursor = self.db_conn.execute(list_rows_query)
        return cursor.fetchall()
    
    def extend_keys(self, table: str, keys: list):
        raise Exception()
    
    def remove_keys(self, table: str, keys: list):
        raise Exception()
    
    def clear_table(self, table: str):
        raise Exception()
    
    def close(self):
        self.db_conn.close()