# coding: utf8
import unittest

from pypika import Query, Table, Tables, JoinException, fn, JoinType, UnionException, Interval

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"
__version__ = "0.0.1"


class JoinTypeTests(unittest.TestCase):
    t0, t1, t2 = Tables('abc', 'efg', 'hij')

    def test_left_join(self):
        q1 = Query.from_(self.t0).join(self.t1).on(self.t0.foo == self.t1.bar).select('*')
        q2 = Query.from_(self.t0).join(self.t1, how=JoinType.left).on(self.t0.foo == self.t1.bar).select('*')

        self.assertEqual('SELECT * FROM abc t0 JOIN efg t1 ON t0.foo=t1.bar', str(q1))
        self.assertEqual('SELECT * FROM abc t0 JOIN efg t1 ON t0.foo=t1.bar', str(q2))

    def test_right_join(self):
        q = Query.from_(self.t0).join(self.t1, how=JoinType.right).on(self.t0.foo == self.t1.bar).select('*')

        self.assertEqual('SELECT * FROM abc t0 RIGHT JOIN efg t1 ON t0.foo=t1.bar', str(q))

    def test_inner_join(self):
        q = Query.from_(self.t0).join(self.t1, how=JoinType.inner).on(self.t0.foo == self.t1.bar).select('*')

        self.assertEqual('SELECT * FROM abc t0 INNER JOIN efg t1 ON t0.foo=t1.bar', str(q))

    def test_outer_join(self):
        q = Query.from_(self.t0).join(self.t1, how=JoinType.outer).on(self.t0.foo == self.t1.bar).select('*')

        self.assertEqual('SELECT * FROM abc t0 OUTER JOIN efg t1 ON t0.foo=t1.bar', str(q))

    def test_join_arithmetic_field(self):
        q = Query.from_(self.t0).join(self.t1).on(self.t0.dt == (self.t1.dt - Interval(weeks=1))).select('*')

        self.assertEqual('SELECT * FROM abc t0 JOIN efg t1 ON t0.dt=t1.dt-INTERVAL 1 WEEK', str(q))

    def test_join_with_arithmetic_function_in_select(self):
        q = Query.from_(
            self.t0,
        ).join(self.t1).on(
            self.t0.dt == (self.t1.dt - Interval(weeks=1))
        ).select(self.t0.fiz - self.t0.buz, self.t1.star)

        self.assertEqual('SELECT t0.fiz-t0.buz,t1.* FROM abc t0 JOIN efg t1 ON t0.dt=t1.dt-INTERVAL 1 WEEK', str(q))

    def test_join_on_complex_criteria(self):
        q = Query.from_(self.t0).join(self.t1, how=JoinType.right).on(
            (self.t0.foo == self.t1.fiz) & (self.t0.bar == self.t1.buz)
        ).select('*')

        self.assertEqual('SELECT * FROM abc t0 RIGHT JOIN efg t1 ON t0.foo=t1.fiz AND t0.bar=t1.buz', str(q))


class JoinBehaviorTests(unittest.TestCase):
    t0, t1, t2, t3 = Tables('abc', 'efg', 'hij', 'klm')

    def test_select__ordered_select_clauses(self):
        q = Query.from_(self.t0).join(self.t1).on(self.t0.foo == self.t1.bar).select(
            self.t0.baz,
            self.t1.buz,
            self.t0.fiz,
            self.t1.bam,
        )

        self.assertEqual('SELECT t0.baz,t1.buz,t0.fiz,t1.bam FROM abc t0 '
                         'JOIN efg t1 ON t0.foo=t1.bar', str(q))

    def test_select__star_for_table(self):
        q = Query.from_(self.t0).join(self.t1).on(
            self.t0.foo == self.t1.bar
        ).join(self.t2).on(
            self.t0.buz == self.t2.bam
        ).select(self.t0.star).select(self.t1.star).select(self.t2.star)

        self.assertEqual('SELECT t0.*,t1.*,t2.* FROM abc t0 '
                         'JOIN efg t1 ON t0.foo=t1.bar '
                         'JOIN hij t2 ON t0.buz=t2.bam', str(q))

    def test_select__star_for_table__replacement(self):
        q = Query.from_(self.t0).join(self.t1).on(
            self.t0.foo == self.t1.bar
        ).join(self.t2).on(
            self.t0.buz == self.t2.bam
        ).select(
            self.t0.foo, self.t1.bar, self.t2.bam
        ).select(self.t0.star).select(self.t1.star).select(self.t2.star)

        self.assertEqual('SELECT t0.*,t1.*,t2.* FROM abc t0 '
                         'JOIN efg t1 ON t0.foo=t1.bar '
                         'JOIN hij t2 ON t0.buz=t2.bam', str(q))

    def test_select_fields_with_where(self):
        q = Query.from_(self.t0).join(
            self.t1).on(self.t0.foo == self.t1.bar
                        ).join(
            self.t2).on(self.t0.buz == self.t2.bam
                        ).select(
            self.t0.foo, self.t1.bar, self.t2.bam
        ).where(self.t0.foo > 1).where(self.t1.bar != 2)

        self.assertEqual('SELECT t0.foo,t1.bar,t2.bam FROM abc t0 '
                         'JOIN efg t1 ON t0.foo=t1.bar '
                         'JOIN hij t2 ON t0.buz=t2.bam '
                         'WHERE t0.foo>1 AND t1.bar<>2', str(q))

    def test_require_condition(self):
        with self.assertRaises(JoinException):
            Query.from_(self.t0).join(self.t1).on(None)

    def test_require_condition_with_both_tables(self):
        with self.assertRaises(JoinException):
            Query.from_(self.t0).join(self.t1).on(self.t0.foo == self.t2.bar)

        with self.assertRaises(JoinException):
            Query.from_(self.t0).join(self.t1).on(self.t2.foo == self.t1.bar)

        with self.assertRaises(JoinException):
            Query.from_(self.t0).join(self.t1).on(self.t2.foo == self.t3.bar)

    def test_join_same_table(self):
        t1 = Table('abc')
        q = Query.from_(self.t0).join(t1).on(self.t0.foo == t1.bar).select(self.t0.foo, t1.buz)

        self.assertEqual('SELECT t0.foo,t1.buz FROM abc t0 JOIN abc t1 ON t0.foo=t1.bar', str(q))

    def test_join_table_twice(self):
        t1, t2 = Tables('efg', 'efg')
        q = Query.from_(self.t0).join(
            t1).on(self.t0.foo == t1.bar
                   ).join(
            t2).on(self.t0.foo == t2.bam
                   ).select(self.t0.foo, t1.fiz, t2.buz)

        self.assertEqual('SELECT t0.foo,t1.fiz,t2.buz FROM abc t0 '
                         'JOIN efg t1 ON t0.foo=t1.bar '
                         'JOIN efg t2 ON t0.foo=t2.bam', str(q))

    def test_select__fields_after_table_star(self):
        q = Query.from_(self.t0).join(self.t1).on(self.t0.foo == self.t1.bar).select(self.t0.star, self.t1.bar).select(
            self.t0.foo)

        self.assertEqual('SELECT t0.*,t1.bar FROM abc t0 JOIN efg t1 ON t0.foo=t1.bar', str(q))

    def test_fail_when_joining_unknown_type(self):
        with self.assertRaises(ValueError):
            Query.from_(self.t0).join('this is a string')

    def test_immutable__queries_after_join(self):
        q0 = Query.from_(self.t0).select(self.t0.foo)
        q1 = q0.join(self.t1).on(self.t0.foo == self.t1.bar).select(self.t1.buz)

        self.assertEqual('SELECT foo FROM abc', str(q0))
        self.assertEqual('SELECT t0.foo,t1.buz FROM abc t0 JOIN efg t1 ON t0.foo=t1.bar', str(q1))

    def test_immutable__tables(self):
        q0 = Query.from_(self.t0).select(self.t0.foo)
        q1 = Query.from_(self.t0).join(self.t1).on(self.t0.foo == self.t1.bar).select(self.t0.foo, self.t1.buz)

        self.assertEqual('SELECT t0.foo,t1.buz FROM abc t0 JOIN efg t1 ON t0.foo=t1.bar', str(q1))
        self.assertEqual('SELECT foo FROM abc', str(q0))

    def test_select_field_from_missing_table(self):
        with self.assertRaises(JoinException):
            Query.from_(self.t0).select(self.t1.foo)

        with self.assertRaises(JoinException):
            Query.from_(self.t0).where(self.t1.foo == 0)

        with self.assertRaises(JoinException):
            Query.from_(self.t0).where(fn.Sum(self.t1.foo) == 0)

        with self.assertRaises(JoinException):
            Query.from_(self.t0).select(fn.Sum(self.t0.bar * 2) + fn.Sum(self.t1.foo * 2))

        with self.assertRaises(JoinException):
            Query.from_(self.t0).groupby(self.t1.foo)

        with self.assertRaises(JoinException):
            Query.from_(self.t0).groupby(self.t0.foo).having(self.t1.bar)

    def test_prefixes_added_to_groupby(self):
        test_query = Query.from_(self.t0).join(self.t1).on(
            self.t0.foo == self.t1.bar
        ).select(self.t0.foo, fn.Sum(self.t1.buz)).groupby(self.t0.foo)

        self.assertEqual('SELECT t0.foo,SUM(t1.buz) FROM abc t0 '
                         'JOIN efg t1 ON t0.foo=t1.bar '
                         'GROUP BY t0.foo', str(test_query))

    def test_prefixes_added_to_orderby(self):
        test_query = Query.from_(self.t0).join(self.t1).on(
            self.t0.foo == self.t1.bar
        ).select(self.t0.foo, self.t1.buz).orderby(self.t0.foo)

        self.assertEqual('SELECT t0.foo,t1.buz FROM abc t0 '
                         'JOIN efg t1 ON t0.foo=t1.bar '
                         'ORDER BY t0.foo', str(test_query))

    def test_prefixes_added_to_function_in_orderby(self):
        test_query = Query.from_(self.t0).join(self.t1).on(
            self.t0.foo == self.t1.bar
        ).select(self.t0.foo, self.t1.buz).orderby(fn.Date(self.t0.foo))

        self.assertEqual('SELECT t0.foo,t1.buz FROM abc t0 '
                         'JOIN efg t1 ON t0.foo=t1.bar '
                         'ORDER BY DATE(t0.foo)', str(test_query))


class UnionTests(unittest.TestCase):
    t0, t1 = Tables('abc', 'efg')

    def test_union(self):
        q0 = Query.from_(self.t0).select(self.t0.foo)
        q1 = Query.from_(self.t1).select(self.t1.bar)

        self.assertEqual('SELECT foo FROM abc UNION SELECT bar FROM efg', str(q0 + q1))
        self.assertEqual('SELECT foo FROM abc UNION SELECT bar FROM efg', str(q0.union(q1)))

    def test_union_all(self):
        q0 = Query.from_(self.t0).select(self.t0.foo)
        q1 = Query.from_(self.t1).select(self.t1.bar)

        self.assertEqual('SELECT foo FROM abc UNION ALL SELECT bar FROM efg', str(q0 * q1))
        self.assertEqual('SELECT foo FROM abc UNION ALL SELECT bar FROM efg', str(q0.union_all(q1)))

    def test_require_equal_number_of_fields(self):
        q0 = Query.from_(self.t0).select(self.t0.foo)
        q1 = Query.from_(self.t1).select(self.t1.fiz, self.t1.buz)

        with self.assertRaises(UnionException):
            str(q0 + q1)
