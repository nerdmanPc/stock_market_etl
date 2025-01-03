import sqlite3 as sql3
from datetime import date
import re

DATE_PATTERN = re.compile(r'^([0-9]*)-([0-9]*)-([0-9]*)$')

def decode_cell(cell: str) -> str|date:
    match_ = DATE_PATTERN.match(str(cell))
    if not match_:
        return cell
    year, month, day = match_.groups()
    return date(int(year), int(month), int(day))

def decode_row(row: sql3.Row) -> tuple:
    return tuple(map(decode_cell, row))

def connect_sqlite(db_url):
    return sql3.connect(db_url)

class Warehouse:
    def __init__(self, db_url: str='', db_conn: sql3.Connection=None) -> None:
        self.db_conn = db_conn or sql3.connect(db_url)

    def table_columns(self, table_id: str):
        table_info_query = f"PRAGMA table_info({table_id})"
        columns =  self.db_conn.execute(table_info_query).fetchall()
        if not columns:
            raise Exception(f'Table "{table_id}" does not exist!')
        return columns
    
    def table_primary_keys(self, table_id: str) -> list[str]:
        columns = self.table_columns(table_id)

        is_primary_key = lambda x: x[-1] != 0
        get_key_position = lambda x: x[-1]
        get_name = lambda x: x[1]
        
        return map(get_name, sorted(
            filter(is_primary_key, columns), 
            key=get_key_position
        ))
    
    def latest_timestamp(self, table_id: str, tick: str) -> date:
        query = f"SELECT timestamp FROM {table_id} WHERE tick = '{tick}' ORDER BY timestamp DESC LIMIT 1"
        result = self.db_conn.execute(query).fetchall()
        if not result:
            return date.min
        return date.fromisoformat(result[0][0])

    def insert_rows(self, table_id: str, data: list, table_width: int, if_exists: str) -> None:
        placeholders = ','.join(['?'] * table_width)
        query = f'INSERT OR {if_exists} INTO {table_id} VALUES ({placeholders})'
        self.db_conn.executemany(query, data)
        self.db_conn.commit()

    def extend_table(self, table: str, data: list) -> None:
        columns = self.table_columns(table)
        self.insert_rows(table, data, table_width=len(columns), if_exists='IGNORE')

    def update_table(self, table: str, data: list) -> None:
        columns = self.table_columns(table)
        self.insert_rows(table, data, table_width=len(columns), if_exists='REPLACE')
    
    def list_rows(self, table: str) -> list:
        list_rows_query = f'SELECT * FROM {table}'
        cursor = self.db_conn.execute(list_rows_query)
        return list(map(decode_row, cursor))#.fetchall()

    def list_keys(self, table: str) -> list:
        key_cols = self.table_primary_keys(table)
        key_cols = ','.join(key_cols)
        list_keys_query = f'SELECT {key_cols} FROM {table}'
        cursor = self.db_conn.execute(list_keys_query)
        return cursor.fetchall()
    
    def close(self):
        self.db_conn.close()