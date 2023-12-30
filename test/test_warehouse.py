from unittest import TestCase, main
import sqlite3 as sql3
from src.av_warehouse import Warehouse
from datetime import date
class WarehouseSetup:
    def __init__(self) -> None:

        self.table = 'test_table'
        self.empty_table = 'empty_table'

        self.old_data = [
            ('AAPL', date(2007, 5, 1)), 
            ('NVDA', date(2007, 5, 2)), 
            ('IBM', date(2007, 5, 3))
        ]
        self.new_data = [
            ('TSLA', date(2007, 5, 1)), 
            ('GOOG', date(2007, 5, 1)), 
            ('AMD', date(2007, 5, 1))
        ]

        test_db = sql3.connect(':memory:')
        test_db.execute(f'CREATE TABLE {self.table} (key PRIMARY KEY, value)')
        test_db.execute(f'CREATE TABLE {self.empty_table} (key PRIMARY KEY, value)')

        test_db.executemany(f'INSERT INTO {self.table} VALUES (?, ?)', self.old_data)
        test_db.commit()

        self.warehouse = self.warehouse = Warehouse(test_db)

class ExtendTable(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.setup = WarehouseSetup()

    def test_zero(self):
        setup = self.setup
        setup.warehouse.extend_table(setup.table, setup.new_data[:0])
        new_state = setup.warehouse.list_rows(setup.table)
        self.assertEqual(setup.old_data, new_state)

    def test_one(self):
        setup = self.setup
        setup.warehouse.extend_table(setup.table, setup.new_data[:1])
        new_state = setup.warehouse.list_rows(setup.table)
        self.assertEqual(setup.old_data + setup.new_data[:1], new_state)

    def test_many(self):
        setup = self.setup
        setup.warehouse.extend_table(setup.table, setup.new_data[:3])
        new_state = setup.warehouse.list_rows(setup.table)
        self.assertEqual(setup.old_data + setup.new_data[:3], new_state)

    def test_one_overlap(self):
        setup = self.setup
        setup.warehouse.extend_table(setup.table, [(setup.old_data[0][0], 9), setup.new_data[1]])
        new_state = setup.warehouse.list_rows(setup.table)
        self.assertEqual(setup.old_data + [setup.new_data[1]], new_state)

    def test_many_overlap(self):
        setup = self.setup
        setup.warehouse.extend_table(setup.table, [(setup.old_data[0][0], 9), (setup.old_data[1][0], 8), setup.new_data[1]])
        new_state = setup.warehouse.list_rows(setup.table)
        self.assertEqual(setup.old_data + [setup.new_data[1]], new_state)

    def test_absent_table(self):
        setup = self.setup
        with self.assertRaises(Exception):
            setup.warehouse.extend_table('absent_table', self.new_data.iloc[0:3,:])

    def tearDown(self) -> None:
        super().tearDown()
        self.setup.warehouse.close()

class Listkeys(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.setup = WarehouseSetup()

    def test_empty_table(self):
        setup = self.setup
        keys = setup.warehouse.list_keys(setup.empty_table)
        self.assertEqual(keys, [])

    def test_full_table(self):
        setup = self.setup
        keys = setup.warehouse.list_keys(setup.table)
        self.assertEqual(sorted(keys), sorted([(key,) for (key, _) in setup.old_data]))

    def test_absent_table(self):
        setup = self.setup
        with self.assertRaises(Exception):
            setup.warehouse.list_keys('absent_table')

    def tearDown(self) -> None:
        super().tearDown()
        self.setup.warehouse.close()

if __name__ == '__main__':
    main()