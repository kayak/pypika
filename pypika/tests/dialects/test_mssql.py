import unittest

from pypika import Table
from pypika.dialects import MSSQLQuery
from pypika.utils import QueryException


class SelectTests(unittest.TestCase):
    def test_normal_select(self):
        q = MSSQLQuery.from_("abc").select("def")

        self.assertEqual('SELECT "def" FROM "abc"', str(q))

    def test_distinct_select(self):
        q = MSSQLQuery.from_("abc").select("def").distinct()

        self.assertEqual('SELECT DISTINCT "def" FROM "abc"', str(q))

    def test_top_distinct_select(self):
        q = MSSQLQuery.from_("abc").select("def").top(10).distinct()

        self.assertEqual('SELECT DISTINCT TOP (10) "def" FROM "abc"', str(q))

    def test_top_select(self):
        q = MSSQLQuery.from_("abc").select("def").top(10)

        self.assertEqual('SELECT TOP (10) "def" FROM "abc"', str(q))

    def test_top_select_non_int(self):
        with self.assertRaisesRegex(QueryException, "TOP value must be an integer"):
            MSSQLQuery.from_("abc").select("def").top("a")
