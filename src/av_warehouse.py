import pandas as pd
import sqlite3 as sql3

def connect(db_path):
    return sql3.connect(db_path)
class Warehouse:
    def __init__(self, db_conn) -> None:
        self.db_conn = db_conn

    def table_columns(self, table_id: str):
        table_info_query = f"PRAGMA table_info({table_id})"
        return self.db_conn.execute(table_info_query).fetchall()
    
    def table_primary_keys(self, table_id: str):
        columns = self.table_columns(table_id)

        is_primary_key = lambda x: x[-1] != 0
        get_primary_key = lambda x: x[-1]
        get_name = lambda x: x[1]
        
        return map(get_name, sorted(
            filter(is_primary_key, columns), 
            key=get_primary_key
        ))
    
    def insertion_query(self, table_id: str, table_width: int, if_exists: str) -> str:
        placeholders = ','.join(['?'] * table_width)
        return f'INSERT OR {if_exists} INTO {table_id} VALUES ({placeholders})'

    def extend_table(self, table: str, data: list):
        columns = self.table_columns(table)
        #dbg = { col[1]: value for col, value in zip(columns, data[0]) }
        extend_query = self.insertion_query(table, table_width=len(columns), if_exists='IGNORE')
        self.db_conn.executemany(extend_query, data)
        self.db_conn.commit()

    def update_table(self, table: str, data: list):
        columns = self.table_columns(table)
        #dbg = { col[1]: value for col, value in zip(columns, data[0]) }
        update_query = self.insertion_query(table, table_width=len(columns), if_exists='REPLACE')
        self.db_conn.executemany(update_query, data)
        self.db_conn.commit()
    
    def list_keys(self, table: str) -> list:
        key_cols = self.table_primary_keys(table)
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