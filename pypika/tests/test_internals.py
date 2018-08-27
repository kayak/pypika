import unittest

from pypika import (
    Case,
    Field,
    Table,
)


class TablesTests(unittest.TestCase):

    def test__criterion_for_with_value(self):
        table = Table('a')

        c = (Field('foo') > 1).for_(table)
        self.assertEqual(c.left, table)
        self.assertEqual(c.tables_, {table})

    def test__case_tables(self):
        table = Table('a')

        c = Case().when(table.a == 1, 2 * table.a)
        self.assertIsInstance(c.tables_, set)
        self.assertSetEqual(c.tables_, {table})
