import sqlite3 as sql3
import os

WAREHOUSE_PATH = os.environ['ETL_WAREHOUSE_PATH']

migration = open('migration/create_tables.sql').read()
setup_companies_list = open('migration/add_companies.sql').read()

memory_db = sql3.connect(WAREHOUSE_PATH)
memory_db.executescript(migration)
memory_db.executescript(setup_companies_list)
memory_db.close()