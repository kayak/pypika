import unittest

from pypika import (
    Array,
    Bracket,
    Dialects,
    PostgreSQLQuery,
    Query,
    Tables,
    Tuple,
)


class TupleTests(unittest.TestCase):
    table_abc, table_efg = Tables("abc", "efg")

    def test_tuple_equality_tuple_on_both(self):
        q = (
            Query.from_(self.table_abc)
            .select(self.table_abc.foo, self.table_abc.bar)
            .where(Tuple(self.table_abc.foo, self.table_abc.bar) == Tuple(1, 2))
        )

        self.assertEqual(
            'SELECT "foo","bar" FROM "abc" ' 'WHERE ("foo","bar")=(1,2)', str(q)
        )

    def test_tuple_equality_tuple_on_left(self):
        q = (
            Query.from_(self.table_abc)
            .select(self.table_abc.foo, self.table_abc.bar)
            .where(Tuple(self.table_abc.foo, self.table_abc.bar) == (1, 2))
        )

        self.assertEqual(
            'SELECT "foo","bar" FROM "abc" ' 'WHERE ("foo","bar")=(1,2)', str(q)
        )

    def test_tuple_equality_tuple_on_right(self):
        q = (
            Query.from_(self.table_abc)
            .select(self.table_abc.foo, self.table_abc.bar)
            .where((self.table_abc.foo, self.table_abc.bar) == Tuple(1, 2))
        )

        # Order is reversed due to lack of right equals method
        self.assertEqual(
            'SELECT "foo","bar" FROM "abc" ' 'WHERE (1,2)=("foo","bar")', str(q)
        )

    def test_tuple_in_using_python_tuples(self):
        q = (
            Query.from_(self.table_abc)
            .select(self.table_abc.foo, self.table_abc.bar)
            .where(
                Tuple(self.table_abc.foo, self.table_abc.bar).isin(
                    [(1, 1), (2, 2), (3, 3)]
                )
            )
        )

        self.assertEqual(
            'SELECT "foo","bar" FROM "abc" '
            'WHERE ("foo","bar") IN ((1,1),(2,2),(3,3))',
            str(q),
        )

    def test_tuple_in_using_pypika_tuples(self):
        q = (
            Query.from_(self.table_abc)
            .select(self.table_abc.foo, self.table_abc.bar)
            .where(
                Tuple(self.table_abc.foo, self.table_abc.bar).isin(
                    [Tuple(1, 1), Tuple(2, 2), Tuple(3, 3)]
                )
            )
        )

        self.assertEqual(
            'SELECT "foo","bar" FROM "abc" '
            'WHERE ("foo","bar") IN ((1,1),(2,2),(3,3))',
            str(q),
        )

    def test_tuple_in_using_mixed_tuples(self):
        q = (
            Query.from_(self.table_abc)
            .select(self.table_abc.foo, self.table_abc.bar)
            .where(
                Tuple(self.table_abc.foo, self.table_abc.bar).isin(
                    [(1, 1), Tuple(2, 2), (3, 3)]
                )
            )
        )

        self.assertEqual(
            'SELECT "foo","bar" FROM "abc" '
            'WHERE ("foo","bar") IN ((1,1),(2,2),(3,3))',
            str(q),
        )

    def test_tuples_in_join(self):
        query = (
            Query.from_(self.table_abc)
            .join(self.table_efg)
            .on(self.table_abc.foo == self.table_efg.bar)
            .select("*")
            .where(
                Tuple(self.table_abc.foo, self.table_efg.bar).isin(
                    [(1, 1), Tuple(2, 2), (3, 3)]
                )
            )
        )

        self.assertEqual(
            'SELECT * FROM "abc" JOIN "efg" ON "abc"."foo"="efg"."bar" '
            'WHERE ("abc"."foo","efg"."bar") IN ((1,1),(2,2),(3,3))',
            str(query),
        )


class ArrayTests(unittest.TestCase):
    table_abc, table_efg = Tables("abc", "efg")

    def test_array_general(self):
        query = Query.from_(self.table_abc).select(Array(1, "a", ["b", 2, 3]))

        self.assertEqual("SELECT [1,'a',['b',2,3]] FROM \"abc\"", str(query))
        self.assertEqual("SELECT [1,'a',['b',2,3]] FROM \"abc\"", str(query.get_sql()))

    def test_array_postgresql(self):
        query = PostgreSQLQuery.from_(self.table_abc).select(Array(1, "a", ["b", 2, 3]))

        self.assertEqual("SELECT ARRAY[1,'a',ARRAY['b',2,3]] FROM \"abc\"", str(query))
        self.assertEqual(
            "SELECT ARRAY[1,'a',ARRAY['b',2,3]] FROM \"abc\"", query.get_sql()
        )


class BracketTests(unittest.TestCase):
    table_abc, table_efg = Tables("abc", "efg")

    def test_arithmetic_with_brackets(self):
        q = Query.from_(self.table_abc).select(Bracket(self.table_abc.foo / 2) / 2)

        self.assertEqual('SELECT ("foo"/2)/2 FROM "abc"', str(q))

    def test_arithmetic_with_brackets_and_alias(self):
        q = Query.from_(self.table_abc).select(
            Bracket(self.table_abc.foo / 2).as_("alias")
        )

        self.assertEqual('SELECT ("foo"/2) "alias" FROM "abc"', str(q))
