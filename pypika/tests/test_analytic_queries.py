# coding: utf-8
import unittest

from pypika import Query, Tables, analytics as an, JoinType

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class RankTests(unittest.TestCase):
    table_abc, table_efg = Tables('abc', 'efg')

    def test_rank(self):
        expr = an.Rank() \
            .over(self.table_abc.foo) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'RANK() '
                         'OVER(PARTITION BY "foo" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_multiple_partitions(self):
        expr = an.Rank() \
            .orderby(self.table_abc.date) \
            .over(self.table_abc.foo, self.table_abc.bar)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'RANK() '
                         'OVER(PARTITION BY "foo","bar" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_ntile_no_partition_or_order_invalid_sql(self):
        expr = an.NTile(5)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'NTILE(5) '
                         'FROM "abc"', str(q))

    def test_ntile_with_partition(self):
        expr = an.NTile(5) \
            .over(self.table_abc.foo)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'NTILE(5) '
                         'OVER(PARTITION BY "foo") '
                         'FROM "abc"', str(q))

    def test_ntile_with_order(self):
        expr = an.NTile(5) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'NTILE(5) '
                         'OVER(ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_ntile_with_partition_and_order(self):
        expr = an.NTile(5) \
            .over(self.table_abc.foo) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'NTILE(5) '
                         'OVER(PARTITION BY "foo" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_first_value(self):
        expr = an.FirstValue(self.table_abc.fizz) \
            .over(self.table_abc.foo) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'FIRST_VALUE("fizz") '
                         'OVER(PARTITION BY "foo" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_first_value_ignore_nulls(self):
        expr = an.FirstValue(self.table_abc.fizz) \
            .over(self.table_abc.foo) \
            .orderby(self.table_abc.date) \
            .ignore_nulls()

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'FIRST_VALUE("fizz" IGNORE NULLS) '
                         'OVER(PARTITION BY "foo" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_first_value_ignore_nulls_first(self):
        expr = an.FirstValue(self.table_abc.fizz) \
            .ignore_nulls() \
            .over(self.table_abc.foo) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'FIRST_VALUE("fizz" IGNORE NULLS) '
                         'OVER(PARTITION BY "foo" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_first_value_multi_argument(self):
        expr = an.FirstValue(self.table_abc.fizz, self.table_abc.buzz) \
            .over(self.table_abc.foo) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'FIRST_VALUE("fizz","buzz") '
                         'OVER(PARTITION BY "foo" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_last_value(self):
        expr = an.LastValue(self.table_abc.fizz) \
            .over(self.table_abc.foo) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'LAST_VALUE("fizz") '
                         'OVER(PARTITION BY "foo" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_last_value_multi_argument(self):
        expr = an.LastValue(self.table_abc.fizz, self.table_abc.buzz) \
            .over(self.table_abc.foo) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'LAST_VALUE("fizz","buzz") '
                         'OVER(PARTITION BY "foo" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_last_value_ignore_nulls(self):
        expr = an.LastValue(self.table_abc.fizz) \
            .over(self.table_abc.foo) \
            .orderby(self.table_abc.date) \
            .ignore_nulls()

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'LAST_VALUE("fizz" IGNORE NULLS) '
                         'OVER(PARTITION BY "foo" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_median(self):
        expr = an.Median(self.table_abc.fizz) \
            .over(self.table_abc.foo, self.table_abc.bar) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'MEDIAN("fizz") '
                         'OVER(PARTITION BY "foo","bar" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_avg(self):
        expr = an.Avg(self.table_abc.fizz) \
            .over(self.table_abc.foo, self.table_abc.bar) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'AVG("fizz") '
                         'OVER(PARTITION BY "foo","bar" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_stddev(self):
        expr = an.StdDev(self.table_abc.fizz) \
            .over(self.table_abc.foo, self.table_abc.bar) \
            .orderby(self.table_abc.date)

        q = Query.from_(self.table_abc).select(expr)

        self.assertEqual('SELECT '
                         'STDDEV("fizz") '
                         'OVER(PARTITION BY "foo","bar" ORDER BY "date") '
                         'FROM "abc"', str(q))

    def test_table_prefixes_used_in_analytic_functions(self):
        expr = an.Rank() \
            .over(self.table_abc.foo) \
            .orderby(self.table_efg.date)

        query = Query.from_(self.table_abc) \
            .join(self.table_efg, how=JoinType.left) \
            .on(self.table_abc.foo == self.table_efg.bar) \
            .select('*', expr)

        self.assertEqual('SELECT *,RANK() OVER(PARTITION BY "abc"."foo" ORDER BY "efg"."date") '
                         'FROM "abc" LEFT JOIN "efg" ON "abc"."foo"="efg"."bar"', str(query))
