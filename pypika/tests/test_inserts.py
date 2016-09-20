# coding: utf8
import unittest

from pypika import Table, Tables, Query

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class InsertIntoTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_insert_one_column(self):
        query = Query.into(self.table_abc).insert(1)

        self.assertEqual('INSERT INTO "abc" VALUES (1)', str(query))

    def test_insert_one_column_single_element_array(self):
        query = Query.into(self.table_abc).insert((1,))

        self.assertEqual('INSERT INTO "abc" VALUES (1)', str(query))

    def test_insert_one_column_multi_element_array(self):
        query = Query.into(self.table_abc).insert((1,), (2,))

        self.assertEqual('INSERT INTO "abc" VALUES (1),(2)', str(query))

    def test_insert_all_columns(self):
        query = Query.into(self.table_abc).insert(1, 'a', True)

        self.assertEqual('INSERT INTO "abc" VALUES (1,\'a\',true)', str(query))

    def test_insert_all_columns_multi_rows_chained(self):
        query = Query.into(self.table_abc).insert(1, 'a', True).insert(2, 'b', False)

        self.assertEqual('INSERT INTO "abc" VALUES (1,\'a\',true),(2,\'b\',false)', str(query))

    def test_insert_all_columns_single_element_arrays(self):
        query = Query.into(self.table_abc).insert((1, 'a', True))

        self.assertEqual('INSERT INTO "abc" VALUES (1,\'a\',true)', str(query))

    def test_insert_all_columns_multi_rows_arrays(self):
        query = Query.into(self.table_abc).insert((1, 'a', True), (2, 'b', False))

        self.assertEqual('INSERT INTO "abc" VALUES (1,\'a\',true),(2,\'b\',false)', str(query))

    def test_insert_all_columns_multi_rows_chained_mixed(self):
        query = Query.into(self.table_abc).insert(
            (1, 'a', True), (2, 'b', False)
        ).insert(3, 'c', True)

        self.assertEqual('INSERT INTO "abc" VALUES '
                         '(1,\'a\',true),(2,\'b\',false),'
                         '(3,\'c\',true)', str(query))

    def test_insert_all_columns_multi_rows_chained_arrays(self):
        query = Query.into(self.table_abc).insert(
            (1, 'a', True), (2, 'b', False)
        ).insert(
            (3, 'c', True), (4, 'd', False)
        )

        self.assertEqual('INSERT INTO "abc" VALUES '
                         '(1,\'a\',true),(2,\'b\',false),'
                         '(3,\'c\',true),(4,\'d\',false)', str(query))

    def test_insert_selected_columns(self):
        query = Query.into(self.table_abc).columns(
            self.table_abc.foo, self.table_abc.bar, self.table_abc.buz
        ).insert(1, 'a', True)

        self.assertEqual('INSERT INTO "abc" ("foo","bar","buz") VALUES (1,\'a\',true)', str(query))

    def test_insert_none_skipped(self):
        query = Query.into(self.table_abc).insert()

        self.assertEqual('', str(query))

    def test_insert_ignore(self):
        query = Query.into(self.table_abc).insert(1).ignore()

        self.assertEqual('INSERT IGNORE INTO "abc" VALUES (1)', str(query))


class InsertSelectFromTests(unittest.TestCase):
    table_abc, table_efg, table_hij = Tables('abc', 'efg', 'hij')

    def test_insert_star(self):
        query = Query.into(self.table_abc).from_(self.table_efg).select('*')

        self.assertEqual('INSERT INTO "abc" SELECT * FROM "efg"', str(query))

    def test_insert_from_columns(self):
        query = Query.into(self.table_abc).from_(self.table_efg).select(
            self.table_efg.fiz, self.table_efg.buz, self.table_efg.baz
        )

        self.assertEqual('INSERT INTO "abc" '
                         'SELECT "fiz","buz","baz" FROM "efg"', str(query))

    def test_insert_columns_from_star(self):
        query = Query.into(self.table_abc).columns(
            self.table_abc.foo, self.table_abc.bar, self.table_abc.buz,
        ).from_(self.table_efg).select('*')

        self.assertEqual('INSERT INTO "abc" ("foo","bar","buz") '
                         'SELECT * FROM "efg"', str(query))

    def test_insert_columns_from_columns(self):
        query = Query.into(self.table_abc).columns(
            self.table_abc.foo, self.table_abc.bar, self.table_abc.buz
        ).from_(self.table_efg).select(
            self.table_efg.fiz, self.table_efg.buz, self.table_efg.baz
        )

        self.assertEqual('INSERT INTO "abc" ("foo","bar","buz") '
                         'SELECT "fiz","buz","baz" FROM "efg"', str(query))

    def test_insert_columns_from_columns_with_join(self):
        query = Query.into(self.table_abc).columns(
            self.table_abc.c1, self.table_abc.c2, self.table_abc.c3, self.table_abc.c4
        ).from_(self.table_efg).select(
            self.table_efg.foo, self.table_efg.bar
        ).join(self.table_hij).on(
            self.table_efg.id == self.table_hij.abc_id
        ).select(
            self.table_hij.fiz, self.table_hij.buz
        )

        self.assertEqual('INSERT INTO "abc" ("c1","c2","c3","c4") '
                         'SELECT "efg"."foo","efg"."bar","hij"."fiz","hij"."buz" FROM "efg" '
                         'JOIN "hij" ON "efg"."id"="hij"."abc_id"', str(query))

    def test_insert_star(self):
        query = Query.into(self.table_abc).from_(self.table_efg).select('*').ignore()

        self.assertEqual('INSERT IGNORE INTO "abc" SELECT * FROM "efg"', str(query))


class SelectIntoTests(unittest.TestCase):
    table_abc, table_efg, table_hij = Tables('abc', 'efg', 'hij')

    def test_select_star_into(self):
        query = Query.from_(self.table_abc).select('*').into(self.table_efg)

        self.assertEqual('SELECT * INTO "efg" FROM "abc"', str(query))

    def test_select_columns_into(self):
        query = Query.from_(self.table_abc).select(
            self.table_abc.foo, self.table_abc.bar, self.table_abc.buz
        ).into(self.table_efg)

        self.assertEqual('SELECT "foo","bar","buz" INTO "efg" FROM "abc"', str(query))

    def test_select_columns_into_with_join(self):
        query = Query.from_(self.table_abc).select(
            self.table_abc.foo, self.table_abc.bar
        ).join(self.table_hij).on(
            self.table_abc.id == self.table_hij.abc_id
        ).select(
            self.table_hij.fiz, self.table_hij.buz
        ).into(self.table_efg)

        self.assertEqual('SELECT "abc"."foo","abc"."bar","hij"."fiz","hij"."buz" '
                         'INTO "efg" FROM "abc" '
                         'JOIN "hij" ON "abc"."id"="hij"."abc_id"', str(query))
