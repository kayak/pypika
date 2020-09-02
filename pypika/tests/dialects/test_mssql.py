import unittest

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

    def test_limit(self):
        q = MSSQLQuery.from_("abc").select("def").orderby("def").limit(10)

        self.assertEqual('SELECT "def" FROM "abc" ORDER BY "def" OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY', str(q))

    def test_fetch_next(self):
        q = MSSQLQuery.from_("abc").select("def").orderby("def").fetch_next(10)

        self.assertEqual('SELECT "def" FROM "abc" ORDER BY "def" OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY', str(q))

    def test_offset(self):
        q = MSSQLQuery.from_("abc").select("def").orderby("def").offset(10)

        self.assertEqual('SELECT "def" FROM "abc" ORDER BY "def" OFFSET 10 ROWS', str(q))

    def test_fetch_next_with_offset(self):
        q = MSSQLQuery.from_("abc").select("def").orderby("def").fetch_next(10).offset(10)

        self.assertEqual('SELECT "def" FROM "abc" ORDER BY "def" OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY', str(q))
