import unittest

from pypika import Table
from pypika.dialects import PostgreSQLQuery


class InsertTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_array_keyword(self):
        q = PostgreSQLQuery.into(self.table_abc).insert(1, [1, "a", True])

        self.assertEqual('INSERT INTO "abc" VALUES (1,ARRAY[1,\'a\',true])', str(q))