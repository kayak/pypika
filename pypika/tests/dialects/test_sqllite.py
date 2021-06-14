import unittest

from pypika import Table
from pypika.dialects import SQLLiteQuery


class SelectTests(unittest.TestCase):
    table_abc = Table("abc")

    def test_bool_true_as_one(self):
        q = SQLLiteQuery.from_("abc").select(True)

        self.assertEqual('SELECT 1 FROM "abc"', str(q))

    def test_bool_false_as_zero(self):
        q = SQLLiteQuery.from_("abc").select(False)

        self.assertEqual('SELECT 0 FROM "abc"', str(q))


class ReplaceTests(unittest.TestCase):
    def test_normal_replace(self):
        query = SQLLiteQuery.into("abc").replace("v1", "v2", "v3")
        expected_output = "REPLACE INTO \"abc\" VALUES ('v1','v2','v3')"
        self.assertEqual(expected_output, str(query))

    def test_replace_subquery(self):
        query = SQLLiteQuery.into("abc").replace(SQLLiteQuery.from_("def").select("f1", "f2"))
        expected_output = 'REPLACE INTO "abc" VALUES ((SELECT "f1","f2" FROM "def"))'
        self.assertEqual(expected_output, str(query))

    def test_insert_or_replace(self):
        query = SQLLiteQuery.into("abc").insert_or_replace("v1", "v2", "v3")
        expected_output = "INSERT OR REPLACE INTO \"abc\" VALUES ('v1','v2','v3')"
        self.assertEqual(expected_output, str(query))
