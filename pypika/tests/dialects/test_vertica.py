import unittest

from pypika import (
    Table,
    Tables,
    Columns,
    VerticaQuery,
)


class VerticaQueryTests(unittest.TestCase):
    table_abc = Table("abc")

    def test_select_query_with_hint(self):
        q = VerticaQuery.from_("abc").select("*").hint("test_hint")

        self.assertEqual('SELECT /*+label(test_hint)*/ * FROM "abc"', str(q))

    def test_insert_query_with_hint(self):
        query = VerticaQuery.into(self.table_abc).insert(1).hint("test_hint")

        self.assertEqual(
            'INSERT /*+label(test_hint)*/ INTO "abc" VALUES (1)', str(query)
        )

    def test_update_query_with_hint(self):
        q = VerticaQuery.update(self.table_abc).set("foo", "bar").hint("test_hint")

        self.assertEqual('UPDATE /*+label(test_hint)*/ "abc" SET "foo"=\'bar\'', str(q))

    def test_delete_query_with_hint(self):
        q = VerticaQuery.from_("abc").delete().hint("test_hint")

        self.assertEqual('DELETE /*+label(test_hint)*/ FROM "abc"', str(q))


class CopyCSVTests(unittest.TestCase):
    table_abc = Table("abc")

    def test_copy_from_file(self):
        with self.subTest("with table as string"):
            q = VerticaQuery.from_file("/path/to/file").copy_("abc")

            self.assertEqual(
                "COPY \"abc\" FROM LOCAL '/path/to/file' PARSER fcsvparser(header=false)",
                str(q),
            )

        with self.subTest("with table"):
            q = VerticaQuery.from_file("/path/to/file").copy_(self.table_abc)

            self.assertEqual(
                "COPY \"abc\" FROM LOCAL '/path/to/file' PARSER fcsvparser(header=false)",
                str(q),
            )


class CreateTemporaryTableTests(unittest.TestCase):
    new_table, existing_table = Tables("abc", "efg")
    foo, bar = Columns(("a", "INT"), ("b", "VARCHAR(100)"))
    select = VerticaQuery.from_(existing_table).select(
        existing_table.foo, existing_table.bar
    )

    def test_create_local_temporary_table(self):
        with self.subTest("with columns"):
            q = (
                VerticaQuery.create_table(self.new_table)
                .temporary()
                .local()
                .columns(self.foo, self.bar)
            )

            self.assertEqual(
                'CREATE LOCAL TEMPORARY TABLE "abc" ("a" INT,"b" VARCHAR(100))', str(q)
            )

        with self.subTest("with select"):
            q = (
                VerticaQuery.create_table(self.new_table)
                .temporary()
                .local()
                .as_select(self.select)
            )

            self.assertEqual(
                'CREATE LOCAL TEMPORARY TABLE "abc" AS (SELECT "foo","bar" FROM "efg")',
                str(q),
            )

    def test_create_local_table_without_temporary_raises_error(self):
        with self.assertRaises(AttributeError):
            VerticaQuery.create_table(self.new_table).local()

    def test_create_temporary_table_preserve_rows(self):
        with self.subTest("with columns"):
            q = (
                VerticaQuery.create_table(self.new_table)
                .temporary()
                .preserve_rows()
                .columns(self.foo, self.bar)
            )

            self.assertEqual(
                'CREATE TEMPORARY TABLE "abc" ("a" INT,"b" VARCHAR(100)) ON COMMIT PRESERVE ROWS',
                str(q),
            )

        with self.subTest("with select"):
            q = (
                VerticaQuery.create_table(self.new_table)
                .temporary()
                .preserve_rows()
                .as_select(self.select)
            )

            self.assertEqual(
                'CREATE TEMPORARY TABLE "abc" ON COMMIT PRESERVE ROWS AS (SELECT "foo","bar" FROM "efg")',
                str(q),
            )

    def test_create_table_preserve_rows_without_temporary_raises_error(self):
        with self.assertRaises(AttributeError):
            VerticaQuery.create_table(self.new_table).preserve_rows()
