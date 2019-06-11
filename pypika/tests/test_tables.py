import unittest

from pypika import (
    Schema,
    Table,
    Tables
)

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class TableEqualityTests(unittest.TestCase):
    def test_tables_equal_by_name(self):
        t1 = Table("t")
        t2 = Table("t")

        self.assertEqual(t1, t2)

    def test_tables_equal_by_schema_and_name(self):
        t1 = Table("t", schema='a')
        t2 = Table("t", schema='a')

        self.assertEqual(t1, t2)

    def test_tables_equal_by_schema_and_name_using_schema(self):
        a = Schema('a')
        t1 = Table("t", schema=a)
        t2 = Table("t", schema=a)

        self.assertEqual(t1, t2)

    def test_tables_equal_by_schema_and_name_using_schema_with_parent(self):
        parent = Schema('parent')
        a = Schema('a', parent=parent)
        t1 = Table("t", schema=a)
        t2 = Table("t", schema=a)

        self.assertEqual(t1, t2)

    def test_tables_not_equal_by_schema_and_name_using_schema_with_different_parents(self):
        parent = Schema('parent')
        a = Schema('a', parent=parent)
        t1 = Table("t", schema=a)
        t2 = Table("t", schema=Schema('a'))

        self.assertNotEqual(t1, t2)

    def test_tables_not_equal_with_different_schemas(self):
        t1 = Table("t", schema='a')
        t2 = Table("t", schema='b')

        self.assertNotEqual(t1, t2)

    def test_tables_not_equal_with_different_names(self):
        t1 = Table("t", schema='a')
        t2 = Table("q", schema='a')

        self.assertNotEqual(t1, t2)

    def test_many_tables_with_alias(self):
        tables_data = [('table1', 't1'), ('table2', 't2'), ('table3', 't3')]
        tables = Tables(*tables_data)
        for el in tables:
            self.assertIsNotNone(el.alias)

    def test_many_tables_without_alias(self):
        tables_data = ['table1', 'table2', 'table3']
        tables = Tables(*tables_data)
        for el in tables:
            self.assertIsNone(el.alias)

    def test_many_tables_with_or_not_alias(self):
        tables_data = [('table1', 't1'), ('table2'), 'table3']
        tables = Tables(*tables_data)
        for i in range(len(tables)):
            if isinstance(tables_data[i], tuple):
                self.assertIsNotNone(tables[i].alias)
            else:
                self.assertIsNone(tables[i].alias)
