# coding: utf8
import unittest

from pypika import Table, Query

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class UpdateTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_empty_query(self):
        q = Query.update('abc')

        self.assertEqual('', str(q))

    def test_omit_where(self):
        q = Query.update(self.table_abc).set('foo', 'bar')

        self.assertEqual('UPDATE "abc" SET "foo"=\'bar\'', str(q))

    def test_update__table_schema(self):
        table = Table('abc', 'schema1')
        q = Query.update(table).set(table.foo, 1).where(table.foo == 0)

        self.assertEqual('UPDATE "schema1"."abc" SET "foo"=1 WHERE "foo"=0', str(q))
