import sqlite3
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

    def test_backslashes_in_values_are_literal(self):
        # SQLite treats a backslash as a literal character in string literals,
        # so values are left untouched
        q = SQLLiteQuery.update(self.table_abc).set("foo", "bar\\")

        self.assertEqual('UPDATE "abc" SET "foo"=\'bar\\\'', str(q))


class PaginationTests(unittest.TestCase):
    def test_pagination_executes_in_sqlite(self):
        table = Table("abc")
        query = SQLLiteQuery.from_(table).select(table.id).orderby(table.id)
        cases = [
            (query.offset(2), [(3,), (4,)]),
            (query.offset(0), [(1,), (2,), (3,), (4,)]),
            (query.offset(10), []),
            (query.limit(2).offset(1), [(2,), (3,)]),
            (query.limit(0).offset(1), []),
            (query.limit(2).offset(0), [(1,), (2,)]),
            (query.offset(2).limit(None), [(3,), (4,)]),
        ]
        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE abc (id INTEGER)")
            connection.executemany("INSERT INTO abc VALUES (?)", [(1,), (2,), (3,), (4,)])
            for paginated_query, expected in cases:
                with self.subTest(sql=str(paginated_query)):
                    self.assertEqual(expected, connection.execute(str(paginated_query)).fetchall())

    def test_offset_only_does_not_change_the_limit(self):
        query = SQLLiteQuery.from_("abc").select("id").offset(2)
        self.assertEqual('SELECT "id" FROM "abc" LIMIT -1 OFFSET 2', str(query))
        self.assertEqual('SELECT "id" FROM "abc" LIMIT 1 OFFSET 2', str(query.limit(1)))


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
