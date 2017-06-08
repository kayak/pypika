# coding: utf-8

import unittest

from pypika import Query, Tables, Tuple


class TupleTests(unittest.TestCase):
    table_abc, table_efg = Tables('abc', 'efg')

    def test_tuple_equality_tuple_on_both(self):
        q = Query.from_(self.table_abc) \
            .select(self.table_abc.foo, self.table_abc.bar) \
            .where(Tuple(self.table_abc.foo, self.table_abc.bar) == Tuple(1, 2))

        self.assertEqual('SELECT "foo","bar" FROM "abc" '
                         'WHERE ("foo","bar")=(1,2)', str(q))

    def test_tuple_equality_tuple_on_left(self):
        q = Query.from_(self.table_abc) \
            .select(self.table_abc.foo, self.table_abc.bar) \
            .where(Tuple(self.table_abc.foo, self.table_abc.bar) == (1, 2))

        self.assertEqual('SELECT "foo","bar" FROM "abc" '
                         'WHERE ("foo","bar")=(1,2)', str(q))

    def test_tuple_equality_tuple_on_right(self):
        q = Query.from_(self.table_abc) \
            .select(self.table_abc.foo, self.table_abc.bar) \
            .where((self.table_abc.foo, self.table_abc.bar) == Tuple(1, 2))

        # Order is reversed due to lack of right equals method
        self.assertEqual('SELECT "foo","bar" FROM "abc" '
                         'WHERE (1,2)=("foo","bar")', str(q))

    def test_tuple_in_using_python_tuples(self):
        q = Query.from_(self.table_abc) \
            .select(self.table_abc.foo, self.table_abc.bar) \
            .where(Tuple(self.table_abc.foo, self.table_abc.bar).isin([(1, 1), (2, 2), (3, 3)]))

        self.assertEqual('SELECT "foo","bar" FROM "abc" '
                         'WHERE ("foo","bar") IN ((1,1),(2,2),(3,3))', str(q))

    def test_tuple_in_using_pypika_tuples(self):
        q = Query.from_(self.table_abc) \
            .select(self.table_abc.foo, self.table_abc.bar) \
            .where(Tuple(self.table_abc.foo, self.table_abc.bar).isin([Tuple(1, 1), Tuple(2, 2), Tuple(3, 3)]))

        self.assertEqual('SELECT "foo","bar" FROM "abc" '
                         'WHERE ("foo","bar") IN ((1,1),(2,2),(3,3))', str(q))

    def test_tuple_in_using_mixed_tuples(self):
        q = Query.from_(self.table_abc) \
            .select(self.table_abc.foo, self.table_abc.bar) \
            .where(Tuple(self.table_abc.foo, self.table_abc.bar).isin([(1, 1), Tuple(2, 2), (3, 3)]))

        self.assertEqual('SELECT "foo","bar" FROM "abc" '
                         'WHERE ("foo","bar") IN ((1,1),(2,2),(3,3))', str(q))
