# Most of test cases are copied and modified from test_tables.py

import unittest

from pypika import Database, Dialects, Schema, SQLLiteQuery, SYSTEM_TIME, TableMeta, Field


class TableStructureTests(unittest.TestCase):
    def test_table_sql(self):
        class T(metaclass=TableMeta):
            __table_name__ = "test_table"

        self.assertEqual('"test_table"', str(T))

    def test_table_with_alias(self):
        class T(metaclass=TableMeta):
            __table_name__ = "test_table"
            __alias__ = "my_table"

        self.assertEqual('"test_table" "my_table"', T.get_sql(with_alias=True, quote_char='"'))

    def test_table_with_default_name(self):
        class T(metaclass=TableMeta):
            pass

        self.assertEqual('"T"', T.get_sql(with_alias=True, quote_char='"'))

    def test_table_with_schema(self):
        class T(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = Schema("x_schema")

        self.assertEqual('"x_schema"."test_table"', str(T))

    def test_table_with_field(self):
        class T(metaclass=TableMeta):
            __table_name__ = "test_table"

            f = Field('f')

        self.assertEqual('"f"', T.f.get_sql(with_alias=True, quote_char='"'))
        self.assertEqual(id(T), id(T.f.table))

    def test_table_with_field_and_ailas(self):
        class T(metaclass=TableMeta):
            __table_name__ = "test_table"
            
            f = Field('f', alias='my_f')

        self.assertEqual('"f" "my_f"', T.f.get_sql(with_alias=True, quote_char='"'))
        self.assertEqual(id(T), id(T.f.table))

    def test_table_with_unset_field(self):
        class T(metaclass=TableMeta):
            __table_name__ = "test_table"

        self.assertEqual('"f"', T.f.get_sql(with_alias=True, quote_char='"'))
        self.assertEqual(id(T), id(T.f.table))

    def test_table_with_schema_and_schema_parent(self):
        class T(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = Schema("x_schema", parent=Database("x_db"))

        self.assertEqual('"x_db"."x_schema"."test_table"', str(T))

    def test_table_for_system_time_sql(self):
        with self.subTest("with between criterion"):
            class T(metaclass=TableMeta):
                __table_name__ = "test_table"
            
            table = T.for_(SYSTEM_TIME.between('2020-01-01', '2020-02-01'))

            self.assertEqual('"test_table" FOR SYSTEM_TIME BETWEEN \'2020-01-01\' AND \'2020-02-01\'', str(table))

        with self.subTest("with as of criterion"):
            class T(metaclass=TableMeta):
                __table_name__ = "test_table"

            table = T.for_(SYSTEM_TIME.as_of('2020-01-01'))

            self.assertEqual('"test_table" FOR SYSTEM_TIME AS OF \'2020-01-01\'', str(table))

        with self.subTest("with from to criterion"):
            class T(metaclass=TableMeta):
                __table_name__ = "test_table"

            table = T.for_(SYSTEM_TIME.from_to('2020-01-01', '2020-02-01'))

            self.assertEqual('"test_table" FOR SYSTEM_TIME FROM \'2020-01-01\' TO \'2020-02-01\'', str(table))

    def test_table_for_period_sql(self):
        with self.subTest("with between criterion"):
            class T(metaclass=TableMeta):
                __table_name__ = "test_table"

            table = T.for_(T.valid_period.between('2020-01-01', '2020-02-01'))

            self.assertEqual('"test_table" FOR "valid_period" BETWEEN \'2020-01-01\' AND \'2020-02-01\'', str(table))

        with self.subTest("with as of criterion"):
            class T(metaclass=TableMeta):
                __table_name__ = "test_table"

            table = T.for_(T.valid_period.as_of('2020-01-01'))

            self.assertEqual('"test_table" FOR "valid_period" AS OF \'2020-01-01\'', str(table))

        with self.subTest("with from to criterion"):
            class T(metaclass=TableMeta):
                __table_name__ = "test_table"

            table = T.for_(T.valid_period.from_to('2020-01-01', '2020-02-01'))

            self.assertEqual('"test_table" FOR "valid_period" FROM \'2020-01-01\' TO \'2020-02-01\'', str(table))


class TableEqualityTests(unittest.TestCase):
    def test_tables_equal_by_name(self):
        class T1(metaclass=TableMeta):
            __table_name__ = "test_table"

        class T2(metaclass=TableMeta):
            __table_name__ = "test_table"

        self.assertEqual(T1, T2)

    def test_tables_equal_by_schema_and_name(self):
        class T1(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = "a"

        class T2(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = "a"

        self.assertEqual(T1, T2)

    def test_tables_equal_by_schema_and_name_using_schema(self):
        a = Schema("a")

        class T1(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = a

        class T2(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = a

        self.assertEqual(T1, T2)

    def test_tables_equal_by_schema_and_name_using_schema_with_parent(self):
        parent = Schema("parent")
        a = Schema("a", parent=parent)

        class T1(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = a

        class T2(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = a

        self.assertEqual(T1, T2)

    def test_tables_not_equal_by_schema_and_name_using_schema_with_different_parents(
        self,
    ):
        parent = Schema("parent")
        a = Schema("a", parent=parent)

        class T1(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = a

        class T2(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = Schema("a")

        self.assertNotEqual(T1, T2)

    def test_tables_not_equal_with_different_schemas(self):
        class T1(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = "a"

        class T2(metaclass=TableMeta):
            __table_name__ = "test_table"
            __schema__ = "b"

        self.assertNotEqual(T1, T2)

    def test_tables_not_equal_with_different_names(self):
        class T1(metaclass=TableMeta):
            __table_name__ = "t"
            __schema__ = "a"

        class T2(metaclass=TableMeta):
            __table_name__ = "q"
            __schema__ = "a"


class TableDialectTests(unittest.TestCase):
    def test_table_with_default_query_cls(self):
        class T(metaclass=TableMeta):
            __table_name__ = "t"

        q = T.select("1")
        self.assertIs(q.dialect, None)

    def test_table_with_dialect_query_cls(self):
        class T(metaclass=TableMeta):
            __table_name__ = "t"
            __query_cls__ = SQLLiteQuery

        q = T.select("1")
        self.assertIs(q.dialect, Dialects.SQLLITE)

    def test_table_with_bad_query_cls(self):
        with self.assertRaises(TypeError):
            class T(metaclass=TableMeta):
                __table_name__ = "t"
                __query_cls__ = object