import unittest

from pypika import Table

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

    def test_tables_not_equal_with_different_schemas(self):
        t1 = Table("t", schema='a')
        t2 = Table("t", schema='b')

        self.assertNotEqual(t1, t2)

    def test_tables_not_equal_with_different_names(self):
        t1 = Table("t", schema='a')
        t2 = Table("q", schema='a')

        self.assertNotEqual(t1, t2)
