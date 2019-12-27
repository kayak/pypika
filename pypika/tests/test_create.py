import unittest

from pypika import (
    Tables,
    Columns,
    Query,
)


class CreateTableTests(unittest.TestCase):
    new_table, existing_table = Tables("abc", "efg")
    foo, bar = Columns(("a", "INT"), ("b", "VARCHAR(100)"))

    def test_create_table_with_columns(self):
        with self.subTest("without temporary keyword"):
            q = Query.create_table(self.new_table).columns(self.foo, self.bar)

            self.assertEqual('CREATE TABLE "abc" ("a" INT,"b" VARCHAR(100))', str(q))

        with self.subTest("with temporary keyword"):
            q = (
                Query.create_table(self.new_table)
                .temporary()
                .columns(self.foo, self.bar)
            )

            self.assertEqual(
                'CREATE TEMPORARY TABLE "abc" ("a" INT,"b" VARCHAR(100))', str(q)
            )

    def test_create_table_with_select(self):
        select = Query.from_(self.existing_table).select(
            self.existing_table.foo, self.existing_table.bar
        )

        with self.subTest("without temporary keyword"):
            q = Query.create_table(self.new_table).as_select(select)

            self.assertEqual(
                'CREATE TABLE "abc" AS (SELECT "foo","bar" FROM "efg")', str(q)
            )

        with self.subTest("with temporary keyword"):
            q = Query.create_table(self.new_table).temporary().as_select(select)

            self.assertEqual(
                'CREATE TEMPORARY TABLE "abc" AS (SELECT "foo","bar" FROM "efg")',
                str(q),
            )

    def test_create_table_without_columns_or_select_empty(self):
        q = Query.create_table(self.new_table)

        self.assertEqual("", str(q))

    def test_create_table_with_select_and_columns_fails(self):
        select = Query.from_(self.existing_table).select(
            self.existing_table.foo, self.existing_table.bar
        )

        with self.subTest("for columns before as_select"):
            with self.assertRaises(AttributeError):
                Query.create_table(self.new_table).columns(
                    self.foo, self.bar
                ).as_select(select)

        with self.subTest("for as_select before columns"):
            with self.assertRaises(AttributeError):
                Query.create_table(self.new_table).as_select(select).columns(
                    self.foo, self.bar
                )

    def test_create_table_as_select_not_query_raises_error(self):
        with self.assertRaises(TypeError):
            Query.create_table(self.new_table).as_select("abc")
