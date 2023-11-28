import unittest as test
import src.av_warehouse as wh
import sqlite3 as sql3

class TestState:
    def __init__(self) -> None:

        self.table = 'test_table'
        self.old_data = [('AAPL', 1), ('NVDA', 2), ('IBM', 3)]
        self.new_data = [('TSLA', 4), ('GOOG', 5), ('AMD', 6)]

        test_db = sql3.connect(':memory:')
        test_db.execute(f'CREATE TABLE {self.table} (key PRIMARY KEY, value)')
        test_db.executemany(f'INSERT INTO {self.table} VALUES (?, ?)', self.old_data)

        self.warehouse = self.warehouse = wh.Warehouse(test_db)

class ExtendTable(test.TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.state = TestState()

    def test_zero(self):
        state = self.state
        state.warehouse.extend_table(state.table, state.new_data[:0])
        new_state = state.warehouse.list_rows(state.table)
        self.assertEqual(state.old_data, new_state)

    def test_one(self):
        state = self.state
        state.warehouse.extend_table(state.table, state.new_data[:1])
        new_state = state.warehouse.list_rows(state.table)
        self.assertEqual(state.old_data + state.new_data[:1], new_state)

    def test_many(self):
        state = self.state
        state.warehouse.extend_table(state.table, state.new_data[:3])
        new_state = state.warehouse.list_rows(state.table)
        self.assertEqual(state.old_data + state.new_data[:3], new_state)

    def test_one_overlap(self):
        state = self.state
        state.warehouse.extend_table(state.table, [(state.old_data[0][0], 9), state.new_data[1]])
        new_state = state.warehouse.list_rows(state.table)
        self.assertEqual(state.old_data + [state.new_data[1]], new_state)
        #raise Exception()

    def test_many_overlap(self):
        state = self.state
        state.warehouse.extend_table(state.table, [(state.old_data[0][0], 9), (state.old_data[1][0], 8), state.new_data[1]])
        new_state = state.warehouse.list_rows(state.table)
        self.assertEqual(state.old_data + [state.new_data[1]], new_state)
        #raise Exception()

    def test_absent_table(self):
        state = self.state
        with self.assertRaises(Exception):
            self.warehouse.extend_table('abbsent_table', self.new_data.iloc[0:3,:])

    def tearDown(self) -> None:
        super().tearDown()
        self.state.warehouse.close()

class Listkeys(test.TestCase):

    def setUp(self) -> None:
        super().setUp()
        self._warehouse = wh.Warehouse() 

    def test_zero(self):
        raise Exception()

    def test_one(self):
        raise Exception()

    def test_many(self):
        raise Exception()

    def test_absent_table(self):
        raise Exception()

    def tearDown(self) -> None:
        super().tearDown()
        self._warehouse.close()

class ExtendKeys(test.TestCase):

    def setUp(self) -> None:
        super().setUp()
        self._warehouse = wh.Warehouse() 

    def test_zero(self):
        raise Exception()

    def test_one(self):
        raise Exception()

    def test_many(self):
        raise Exception()

    def test_one_overlap(self):
        raise Exception()

    def test_many_overlap(self):
        raise Exception()

    def test_absent_table(self):
        raise Exception()

    def tearDown(self) -> None:
        super().tearDown()
        self._warehouse.close()

class RemoveKeys(test.TestCase):

    def setUp(self) -> None:
        super().setUp()
        self._warehouse = wh.Warehouse() 

    def test_zero(self):
        raise Exception()

    def test_one(self):
        raise Exception()

    def test_many(self):
        raise Exception()

    def test_one_absent(self):
        raise Exception()

    def test_many_absent(self):
        raise Exception()

    def test_absent_table(self):
        raise Exception()

    def tearDown(self) -> None:
        super().tearDown()
        self._warehouse.close()

class ClearTable(test.TestCase):

    def setUp(self) -> None:
        super().setUp()
        self._warehouse = wh.Warehouse() 

    def test_empty(self):
        raise Exception()

    def test_one(self):
        raise Exception()

    def test_many(self):
        raise Exception()

    def test_absent_table(self):
        raise Exception()

    def tearDown(self) -> None:
        super().tearDown()
        self._warehouse.close()

if __name__ == '__main__':
    test.main()