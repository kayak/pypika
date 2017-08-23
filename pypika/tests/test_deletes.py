# coding: utf8
import unittest

from pypika import Table, Query

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class DeleteTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_omit_where(self):
        q = Query.from_('abc').delete()

        self.assertEqual('DELETE FROM "abc"', str(q))

    def test_omit_where__table_schema(self):
        q = Query.from_(Table('abc', 'schema1')).delete()

        self.assertEqual('DELETE FROM "schema1"."abc"', str(q))

    def test_where_field_equals(self):
        q1 = Query.from_(self.table_abc).where(self.table_abc.foo == self.table_abc.bar).delete()
        q2 = Query.from_(self.table_abc).where(self.table_abc.foo.eq(self.table_abc.bar)).delete()

        self.assertEqual('DELETE FROM "abc" WHERE "foo"="bar"', str(q1))
        self.assertEqual('DELETE FROM "abc" WHERE "foo"="bar"', str(q2))
