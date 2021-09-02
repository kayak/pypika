import unittest

from pypika import (
    AliasedQuery,
    Query,
    Tables,
)


class ImmutabilityTests(unittest.TestCase):
    table_a, table_b, table_c = Tables("a", "b", "c")

    def test_select_returns_new_query_instance(self):
        query_a = Query.from_(self.table_a).select(self.table_a.foo)
        query_b = query_a.select(self.table_a.bar)

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_groupby_returns_new_query_instance(self):
        query_a = Query.from_(self.table_a).select(self.table_a.foo).groupby(self.table_a.foo)
        query_b = query_a.groupby(self.table_a.bar)

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_orderby_return_new_query_instance(self):
        query_a = Query.from_(self.table_a).select(self.table_a.foo).orderby(self.table_a.foo)
        query_b = query_a.orderby(self.table_a.bar)

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_join_return_new_query_instance(self):
        base = Query.from_(self.table_a).select(self.table_a.foo)
        query_a = base.join(self.table_b).on(self.table_a.foo == self.table_b.bar).select(self.table_b.bar)
        query_b = query_a.join(self.table_c).on(self.table_a.foo == self.table_c.baz).select(self.table_c.baz)

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_use_index_return_new_query_instance(self):
        query_a = Query.from_(self.table_a).select(self.table_a.foo).use_index('idx1')
        query_b = query_a.use_index('idx2')

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_force_index_return_new_query_instance(self):
        query_a = Query.from_(self.table_a).select(self.table_a.foo).force_index('idx1')
        query_b = query_a.force_index('idx2')

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_with_return_new_query_instance(self):
        alias_1 = Query.from_(self.table_a).select(self.table_a.foo)
        alias_2 = Query.from_(self.table_b).select(self.table_b.bar)
        query_a = Query.from_(AliasedQuery('a1')).select('foo').with_(alias_1, 'a1')
        query_b = query_a.with_(alias_2, 'a2')

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_insert_into_return_new_query_instance(self):
        query_a = Query.into(self.table_a).insert('foo1')
        query_b = query_a.insert('foo2')

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_replace_into_return_new_query_instance(self):
        query_a = Query.into(self.table_a).replace('foo')
        query_b = query_a.replace('bar')

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_update_set_return_new_query_instance(self):
        query_a = Query.update(self.table_a).set(self.table_a.foo, 'foo')
        query_b = query_a.set(self.table_a.bar, 'bar')

        self.assertIsNot(query_a, query_b)
        self.assertNotEqual(str(query_a), str(query_b))

    def test_queries_after_join(self):
        query1 = Query.from_(self.table_a).select(self.table_a.foo)
        query2 = query1.join(self.table_b).on(self.table_a.foo == self.table_b.bar).select(self.table_b.buz)

        self.assertEqual('SELECT "foo" FROM "a"', str(query1))
        self.assertEqual(
            'SELECT "a"."foo","b"."buz" FROM "a" JOIN "b" ON "a"."foo"="b"."bar"',
            str(query2),
        )

    def test_immutable_kwarg_on_query_builder_disables_immutability(self):
        query0 = Query.from_(self.table_a, immutable=False)
        query1 = query0.select(self.table_a.foo)
        self.assertIs(query0, query1)
