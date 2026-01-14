# Most of test cases are copied and modified from test_tables.py

import unittest

from pypika import Database, Dialects, Schema, SQLLiteQuery, Table, SYSTEM_TIME, table_class, Field


class TableStructureTests(unittest.TestCase):
    def test_table_sql(self):
        @table_class("test_table")
        class T(Table):
            pass

        self.assertEqual('"test_table"', str(T))

    def test_table_with_no_superclass(self):
        with self.assertRaises(TypeError):
            @table_class("test_table")
            class T:
                pass

    def test_table_with_bad_superclass(self):
        with self.assertRaises(TypeError):
            @table_class("test_table")
            class T(object):
                pass

    def test_table_with_alias(self):
        @table_class("test_table")
        class T(Table):
            pass

        table = T.as_("my_table")

        self.assertEqual('"test_table" "my_table"', table.get_sql(with_alias=True, quote_char='"'))

    def test_table_with_schema_arg(self):
        @table_class("test_table", schema=Schema("x_schema"))
        class T(Table):
            pass

        self.assertEqual('"x_schema"."test_table"', str(T))

    def test_table_with_field(self):
        @table_class("test_table")
        class T(Table):
            f = Field('f')

        self.assertEqual('"f"', T.f.get_sql(with_alias=True, quote_char='"'))
        self.assertEqual(id(T), id(T.f.table))

    def test_table_with_field_and_alias(self):
        @table_class("test_table")
        class T(Table):
            f = Field('f', alias='my_f')

        self.assertEqual('"f" "my_f"', T.f.get_sql(with_alias=True, quote_char='"'))
        self.assertEqual(id(T), id(T.f.table))

    def test_table_with_unset_field(self):
        @table_class("test_table")
        class T(Table):
            pass

        self.assertEqual('"f"', T.f.get_sql(with_alias=True, quote_char='"'))
        self.assertEqual(id(T), id(T.f.table))

    def test_table_with_schema_and_schema_parent_arg(self):
        @table_class("test_table", schema=Schema("x_schema", parent=Database("x_db")))
        class T(Table):
            pass

        self.assertEqual('"x_db"."x_schema"."test_table"', str(T))

    def test_table_for_system_time_sql(self):
        with self.subTest("with between criterion"):
            @table_class("test_table")
            class T(Table):
                pass

            table = T.for_(SYSTEM_TIME.between('2020-01-01', '2020-02-01'))

            self.assertEqual('"test_table" FOR SYSTEM_TIME BETWEEN \'2020-01-01\' AND \'2020-02-01\'', str(table))

        with self.subTest("with as of criterion"):
            @table_class("test_table")
            class T(Table):
                pass

            table = T.for_(SYSTEM_TIME.as_of('2020-01-01'))

            self.assertEqual('"test_table" FOR SYSTEM_TIME AS OF \'2020-01-01\'', str(table))

        with self.subTest("with from to criterion"):
            @table_class("test_table")
            class T(Table):
                pass

            table = T.for_(SYSTEM_TIME.from_to('2020-01-01', '2020-02-01'))

            self.assertEqual('"test_table" FOR SYSTEM_TIME FROM \'2020-01-01\' TO \'2020-02-01\'', str(table))

    def test_table_for_period_sql(self):
        with self.subTest("with between criterion"):
            @table_class("test_table")
            class T(Table):
                pass

            table = T.for_(T.valid_period.between('2020-01-01', '2020-02-01'))

            self.assertEqual('"test_table" FOR "valid_period" BETWEEN \'2020-01-01\' AND \'2020-02-01\'', str(table))

        with self.subTest("with as of criterion"):
            @table_class("test_table")
            class T(Table):
                pass

            table = T.for_(T.valid_period.as_of('2020-01-01'))

            self.assertEqual('"test_table" FOR "valid_period" AS OF \'2020-01-01\'', str(table))

        with self.subTest("with from to criterion"):
            @table_class("test_table")
            class T(Table):
                pass

            table = T.for_(T.valid_period.from_to('2020-01-01', '2020-02-01'))

            self.assertEqual('"test_table" FOR "valid_period" FROM \'2020-01-01\' TO \'2020-02-01\'', str(table))


class TableEqualityTests(unittest.TestCase):
    def test_tables_equal_by_name(self):
        @table_class("test_table")
        class T1(Table):
            pass

        @table_class("test_table")
        class T2(Table):
            pass

        self.assertEqual(T1, T2)

    def test_tables_equal_by_schema_and_name(self):
        @table_class("test_table", schema="a")
        class T1(Table):
            pass

        @table_class("test_table", schema="a")
        class T2(Table):
            pass

        self.assertEqual(T1, T2)

    def test_tables_equal_by_schema_and_name_using_schema(self):
        a = Schema("a")

        @table_class("test_table", schema=a)
        class T1(Table):
            pass

        @table_class("test_table", schema=a)
        class T2(Table):
            pass

        self.assertEqual(T1, T2)

    def test_tables_equal_by_schema_and_name_using_schema_with_parent(self):
        parent = Schema("parent")
        a = Schema("a", parent=parent)
        
        @table_class("test_table", schema=a)
        class T1(Table):
            pass

        @table_class("test_table", schema=a)
        class T2(Table):
            pass

        self.assertEqual(T1, T2)

    def test_tables_not_equal_by_schema_and_name_using_schema_with_different_parents(
        self,
    ):
        parent = Schema("parent")
        a = Schema("a", parent=parent)

        @table_class("test_table", schema=a)
        class T1(Table):
            pass

        @table_class("test_table", schema=Schema("a"))
        class T2(Table):
            pass

        self.assertNotEqual(T1, T2)

    def test_tables_not_equal_with_different_schemas(self):

        @table_class("test_table", schema="a")
        class T1(Table):
            pass

        @table_class("test_table", schema="b")
        class T2(Table):
            pass

        self.assertNotEqual(T1, T2)

    def test_tables_not_equal_with_different_names(self):

        @table_class("t", schema="a")
        class T1(Table):
            pass

        @table_class("q", schema="a")
        class T2(Table):
            pass

        self.assertNotEqual(T1, T2)


class TableDialectTests(unittest.TestCase):
    def test_table_with_default_query_cls(self):
        @table_class("test_table")
        class T(Table):
            pass

        q = T.select("1")
        self.assertIs(q.dialect, None)

    def test_table_with_dialect_query_cls(self):

        @table_class("test_table", query_cls=SQLLiteQuery)
        class T(Table):
            pass

        q = T.select("1")
        self.assertIs(q.dialect, Dialects.SQLLITE)

    def test_table_with_bad_query_cls(self):
        with self.assertRaises(TypeError):
            @table_class("test_table", query_cls=object)
            class T(Table):
                pass
