# coding: utf8
import unittest

from pypika import Query, Table, Tables, JoinException, functions as fn, JoinType, UnionException, Interval

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"
__version__ = "0.0.1"


class JoinTypeTests(unittest.TestCase):
    table0, table1, t2 = Tables('abc', 'efg', 'hij')

    def test_left_join(self):
        query2 = Query.from_(self.table0).join(self.table1).on(
            self.table0.foo == self.table1.bar).select('*')
        q2 = Query.from_(self.table0).join(self.table1, how=JoinType.left).on(
            self.table0.foo == self.table1.bar).select('*')

        self.assertEqual('SELECT * FROM "abc" "t0" JOIN "efg" "t1" ON "t0"."foo"="t1"."bar"', str(query2))
        self.assertEqual('SELECT * FROM "abc" "t0" JOIN "efg" "t1" ON "t0"."foo"="t1"."bar"', str(q2))

    def test_right_join(self):
        q = Query.from_(self.table0).join(self.table1, how=JoinType.right).on(
            self.table0.foo == self.table1.bar).select('*')

        self.assertEqual('SELECT * FROM "abc" "t0" RIGHT JOIN "efg" "t1" ON "t0"."foo"="t1"."bar"', str(q))

    def test_inner_join(self):
        q = Query.from_(self.table0).join(self.table1, how=JoinType.inner).on(
            self.table0.foo == self.table1.bar).select('*')

        self.assertEqual('SELECT * FROM "abc" "t0" INNER JOIN "efg" "t1" ON "t0"."foo"="t1"."bar"', str(q))

    def test_outer_join(self):
        q = Query.from_(self.table0).join(self.table1, how=JoinType.outer).on(
            self.table0.foo == self.table1.bar).select('*')

        self.assertEqual('SELECT * FROM "abc" "t0" OUTER JOIN "efg" "t1" ON "t0"."foo"="t1"."bar"', str(q))

    def test_join_arithmetic_field(self):
        q = Query.from_(self.table0).join(self.table1).on(
            self.table0.dt == (self.table1.dt - Interval(weeks=1))).select('*')

        self.assertEqual('SELECT * FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."dt"="t1"."dt"-INTERVAL 1 WEEK', str(q))

    def test_join_with_arithmetic_function_in_select(self):
        q = Query.from_(
            self.table0,
        ).join(self.table1).on(
            self.table0.dt == (self.table1.dt - Interval(weeks=1))
        ).select(self.table0.fiz - self.table0.buz, self.table1.star)

        self.assertEqual('SELECT "t0"."fiz"-"t0"."buz","t1".* FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."dt"="t1"."dt"-INTERVAL 1 WEEK', str(q))

    def test_join_on_complex_criteria(self):
        q = Query.from_(self.table0).join(self.table1, how=JoinType.right).on(
            (self.table0.foo == self.table1.fiz) & (self.table0.bar == self.table1.buz)
        ).select('*')

        self.assertEqual('SELECT * FROM "abc" "t0" '
                         'RIGHT JOIN "efg" "t1" ON "t0"."foo"="t1"."fiz" AND "t0"."bar"="t1"."buz"', str(q))


class JoinBehaviorTests(unittest.TestCase):
    table0, table1, table2, table3 = Tables('abc', 'efg', 'hij', 'klm')

    def test_select__ordered_select_clauses(self):
        q = Query.from_(self.table0).join(self.table1).on(self.table0.foo == self.table1.bar).select(
            self.table0.baz,
            self.table1.buz,
            self.table0.fiz,
            self.table1.bam,
        )

        self.assertEqual('SELECT "t0"."baz","t1"."buz","t0"."fiz","t1"."bam" FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar"', str(q))

    def test_select__star_for_table(self):
        q = Query.from_(self.table0).join(self.table1).on(
            self.table0.foo == self.table1.bar
        ).join(self.table2).on(
            self.table0.buz == self.table2.bam
        ).select(self.table0.star).select(self.table1.star).select(self.table2.star)

        self.assertEqual('SELECT "t0".*,"t1".*,"t2".* FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar" '
                         'JOIN "hij" "t2" ON "t0"."buz"="t2"."bam"', str(q))

    def test_select__star_for_table__replacement(self):
        q = Query.from_(self.table0).join(self.table1).on(
            self.table0.foo == self.table1.bar
        ).join(self.table2).on(
            self.table0.buz == self.table2.bam
        ).select(
            self.table0.foo, self.table1.bar, self.table2.bam
        ).select(
            self.table0.star, self.table1.star, self.table2.star
        )

        self.assertEqual('SELECT "t0".*,"t1".*,"t2".* FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar" '
                         'JOIN "hij" "t2" ON "t0"."buz"="t2"."bam"', str(q))

    def test_select_fields_with_where(self):
        q = Query.from_(self.table0).join(
            self.table1).on(self.table0.foo == self.table1.bar
                            ).join(
            self.table2).on(self.table0.buz == self.table2.bam
                            ).select(
            self.table0.foo, self.table1.bar, self.table2.bam
        ).where(self.table0.foo > 1).where(self.table1.bar != 2)

        self.assertEqual('SELECT "t0"."foo","t1"."bar","t2"."bam" FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar" '
                         'JOIN "hij" "t2" ON "t0"."buz"="t2"."bam" '
                         'WHERE "t0"."foo">1 AND "t1"."bar"<>2', str(q))

    def test_require_condition(self):
        with self.assertRaises(JoinException):
            Query.from_(self.table0).join(self.table1).on(None)

    def test_require_condition_with_both_tables(self):
        with self.assertRaises(JoinException):
            Query.from_(self.table0).join(self.table1).on(self.table0.foo == self.table2.bar)

        with self.assertRaises(JoinException):
            Query.from_(self.table0).join(self.table1).on(self.table2.foo == self.table1.bar)

        with self.assertRaises(JoinException):
            Query.from_(self.table0).join(self.table1).on(self.table2.foo == self.table3.bar)

    def test_join_same_table(self):
        table1 = Table('abc')
        q = Query.from_(self.table0).join(table1).on(self.table0.foo == table1.bar).select(self.table0.foo, table1.buz)

        self.assertEqual('SELECT "t0"."foo","t1"."buz" FROM "abc" "t0" '
                         'JOIN "abc" "t1" ON "t0"."foo"="t1"."bar"', str(q))

    def test_join_table_twice(self):
        table1, table2 = Tables('efg', 'efg')
        q = Query.from_(self.table0).join(
            table1).on(self.table0.foo == table1.bar
                       ).join(
            table2).on(self.table0.foo == table2.bam
                       ).select(self.table0.foo, table1.fiz, table2.buz)

        self.assertEqual('SELECT "t0"."foo","t1"."fiz","t2"."buz" FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar" '
                         'JOIN "efg" "t2" ON "t0"."foo"="t2"."bam"', str(q))

    def test_select__fields_after_table_star(self):
        q = Query.from_(self.table0).join(self.table1).on(self.table0.foo == self.table1.bar).select(self.table0.star,
                                                                                                     self.table1.bar).select(
            self.table0.foo)

        self.assertEqual('SELECT "t0".*,"t1"."bar" FROM "abc" "t0" JOIN "efg" "t1" ON "t0"."foo"="t1"."bar"',
                         str(q))

    def test_fail_when_joining_unknown_type(self):
        with self.assertRaises(ValueError):
            Query.from_(self.table0).join('this is a string')

    def test_immutable__queries_after_join(self):
        query1 = Query.from_(self.table0).select(self.table0.foo)
        query2 = query1.join(self.table1).on(self.table0.foo == self.table1.bar).select(self.table1.buz)

        self.assertEqual('SELECT "foo" FROM "abc"', str(query1))
        self.assertEqual('SELECT "t0"."foo","t1"."buz" FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar"', str(query2))

    def test_immutable__tables(self):
        query1 = Query.from_(self.table0).select(self.table0.foo)
        query2 = Query.from_(self.table0).join(self.table1).on(self.table0.foo == self.table1.bar).select(
            self.table0.foo,
            self.table1.buz)

        self.assertEqual('SELECT "t0"."foo","t1"."buz" FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar"', str(query2))
        self.assertEqual('SELECT "foo" FROM "abc"', str(query1))

    def test_select_field_from_missing_table(self):
        with self.assertRaises(JoinException):
            Query.from_(self.table0).select(self.table1.foo)

        with self.assertRaises(JoinException):
            Query.from_(self.table0).where(self.table1.foo == 0)

        with self.assertRaises(JoinException):
            Query.from_(self.table0).where(fn.Sum(self.table1.foo) == 0)

        with self.assertRaises(JoinException):
            Query.from_(self.table0).select(fn.Sum(self.table0.bar * 2) + fn.Sum(self.table1.foo * 2))

        with self.assertRaises(JoinException):
            Query.from_(self.table0).groupby(self.table1.foo)

        with self.assertRaises(JoinException):
            Query.from_(self.table0).groupby(self.table0.foo).having(self.table1.bar)

    def test_prefixes_added_to_groupby(self):
        test_query = Query.from_(self.table0).join(self.table1).on(
            self.table0.foo == self.table1.bar
        ).select(self.table0.foo, fn.Sum(self.table1.buz)).groupby(self.table0.foo)

        self.assertEqual('SELECT "t0"."foo",SUM("t1"."buz") FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar" '
                         'GROUP BY "t0"."foo"', str(test_query))

    def test_prefixes_added_to_orderby(self):
        test_query = Query.from_(self.table0).join(self.table1).on(
            self.table0.foo == self.table1.bar
        ).select(self.table0.foo, self.table1.buz).orderby(self.table0.foo)

        self.assertEqual('SELECT "t0"."foo","t1"."buz" FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar" '
                         'ORDER BY "t0"."foo"', str(test_query))

    def test_prefixes_added_to_function_in_orderby(self):
        test_query = Query.from_(self.table0).join(self.table1).on(
            self.table0.foo == self.table1.bar
        ).select(self.table0.foo, self.table1.buz).orderby(fn.Date(self.table0.foo))

        self.assertEqual('SELECT "t0"."foo","t1"."buz" FROM "abc" "t0" '
                         'JOIN "efg" "t1" ON "t0"."foo"="t1"."bar" '
                         'ORDER BY DATE("t0"."foo")', str(test_query))


class UnionTests(unittest.TestCase):
    table1, table2 = Tables('abc', 'efg')

    def test_union(self):
        query1 = Query.from_(self.table1).select(self.table1.foo)
        query2 = Query.from_(self.table2).select(self.table2.bar)

        self.assertEqual('SELECT "foo" FROM "abc" UNION SELECT "bar" FROM "efg"', str(query1 + query2))
        self.assertEqual('SELECT "foo" FROM "abc" UNION SELECT "bar" FROM "efg"', str(query1.union(query2)))

    def test_union_all(self):
        query1 = Query.from_(self.table1).select(self.table1.foo)
        query2 = Query.from_(self.table2).select(self.table2.bar)

        self.assertEqual('SELECT "foo" FROM "abc" UNION ALL SELECT "bar" FROM "efg"', str(query1 * query2))
        self.assertEqual('SELECT "foo" FROM "abc" UNION ALL SELECT "bar" FROM "efg"', str(query1.union_all(query2)))

    def test_require_equal_number_of_fields(self):
        query1 = Query.from_(self.table1).select(self.table1.foo)
        query2 = Query.from_(self.table2).select(self.table2.fiz, self.table2.buz)

        with self.assertRaises(UnionException):
            str(query1 + query2)
