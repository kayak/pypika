import unittest

from pypika import (
    Table,
    VerticaQuery,
)

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class VerticaQueryTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_select_query_with_hint(self):
        q = VerticaQuery \
            .from_('abc') \
            .select('*') \
            .hint('test_hint')

        self.assertEqual('SELECT /*+label(test_hint)*/ * FROM "abc"', str(q))

    def test_insert_query_with_hint(self):
        query = VerticaQuery \
            .into(self.table_abc) \
            .insert(1) \
            .hint('test_hint')

        self.assertEqual('INSERT /*+label(test_hint)*/ INTO "abc" VALUES (1)', str(query))

    def test_update_query_with_hint(self):
        q = VerticaQuery \
            .update(self.table_abc) \
            .set('foo', 'bar') \
            .hint('test_hint')

        self.assertEqual('UPDATE /*+label(test_hint)*/ "abc" SET "foo"=\'bar\'', str(q))

    def test_delete_query_with_hint(self):
        q = VerticaQuery \
            .from_('abc') \
            .delete() \
            .hint('test_hint')

        self.assertEqual('DELETE /*+label(test_hint)*/ FROM "abc"', str(q))
