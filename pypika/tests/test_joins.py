# coding: utf8
import unittest

from pypika import Query, Table, Tables, JoinException, functions as fn, JoinType, UnionException, Interval

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class JoinTypeTests(unittest.TestCase):
    table0, table1, hij = Tables('abc', 'efg', 'hij')

    def test_left_join(self):
        query = Query.from_(self.table0).join(self.table1, how=JoinType.left).on(
            self.table0.foo == self.table1.bar).select('*')

        self.assertEqual('SELECT * FROM "abc" LEFT JOIN "efg" ON "abc"."foo"="efg"."bar"', str(query))

    def test_right_join(self):
        q = Query.from_(self.table0).join(self.table1, how=JoinType.right).on(
            self.table0.foo == self.table1.bar).select('*')

        self.assertEqual('SELECT * FROM "abc" RIGHT JOIN "efg" ON "abc"."foo"="efg"."bar"', str(q))

    def test_inner_join(self):
        query = Query.from_(self.table0).join(self.table1).on(
            self.table0.foo == self.table1.bar).select('*')
        query_explicit = Query.from_(self.table0).join(self.table1, how=JoinType.inner).on(
            self.table0.foo == self.table1.bar).select('*')

        self.assertEqual('SELECT * FROM "abc" JOIN "efg" ON "abc"."foo"="efg"."bar"', str(query))
        self.assertEqual('SELECT * FROM "abc" JOIN "efg" ON "abc"."foo"="efg"."bar"', str(query_explicit))

    def test_join_on_field_single(self):
        query = Query.from_(self.table0).join(self.table1).on_field("foo").select('*')
        self.assertEqual('SELECT * FROM "abc" JOIN "efg" ON "abc"."foo"="efg"."foo"', str(query))

    def test_join_on_field_multi(self):
        query = Query.from_(self.table0).join(self.table1).on_field("foo", "bar").select('*')
        self.assertEqual('SELECT * FROM "abc" JOIN "efg" ON "abc"."foo"="efg"."foo" '
                         'AND "abc"."bar"="efg"."bar"', str(query))

    def test_join_on_field_multi_with_extra_join(self):
        query = Query.from_(self.table0)\
            .join(self.hij).on_field("buzz")\
            .join(self.table1).on_field("foo", "bar").select('*')

        self.assertEqual('SELECT * FROM "abc" JOIN "hij" ON "abc"."buzz"="hij"."buzz" '
                         'JOIN "efg" ON "abc"."foo"="efg"."foo" AND "abc"."bar"="efg"."bar"', str(query))

    def test_join_using_string_field_name(self):
        query = Query.from_(self.table0).join(self.table1).using('id').select('*')

        self.assertEqual('SELECT * FROM "abc" JOIN "efg" USING ("id")', str(query))

    def test_join_using_multiple_fields(self):
        query = Query.from_(self.table0).join(self.table1).using('foo', 'bar').select('*')

        self.assertEqual('SELECT * FROM "abc" JOIN "efg" USING ("foo","bar")', str(query))

    def test_outer_join(self):
        query = Query.from_(self.table0).join(self.table1, how=JoinType.outer).on(
            self.table0.foo == self.table1.bar).select('*')

        self.assertEqual('SELECT * FROM "abc" OUTER JOIN "efg" ON "abc"."foo"="efg"."bar"', str(query))

    def test_join_arithmetic_field(self):
        q = Query.from_(self.table0).join(self.table1).on(
            self.table0.dt == (self.table1.dt - Interval(weeks=1))).select('*')

        self.assertEqual('SELECT * FROM "abc" '
                         'JOIN "efg" ON "abc"."dt"="efg"."dt"-INTERVAL \'1 WEEK\'', str(q))

    def test_join_with_arithmetic_function_in_select(self):
        q = Query.from_(
            self.table0,
        ).join(self.table1).on(
            self.table0.dt == (self.table1.dt - Interval(weeks=1))
        ).select(self.table0.fiz - self.table0.buz, self.table1.star)

        self.assertEqual('SELECT "abc"."fiz"-"abc"."buz","efg".* FROM "abc" '
                         'JOIN "efg" ON "abc"."dt"="efg"."dt"-INTERVAL \'1 WEEK\'', str(q))

    def test_join_on_complex_criteria(self):
        q = Query.from_(self.table0).join(self.table1, how=JoinType.right).on(
            (self.table0.foo == self.table1.fiz) & (self.table0.bar == self.table1.buz)
        ).select('*')

        self.assertEqual('SELECT * FROM "abc" '
                         'RIGHT JOIN "efg" ON "abc"."foo"="efg"."fiz" AND "abc"."bar"="efg"."buz"', str(q))


class JoinBehaviorTests(unittest.TestCase):
    table_abc, table_efg, table_hij, table_klm = Tables('abc', 'efg', 'hij', 'klm')

    def test_select__ordered_select_clauses(self):
        q = Query.from_(self.table_abc).join(self.table_efg).on(self.table_abc.foo == self.table_efg.bar).select(
            self.table_abc.baz,
            self.table_efg.buz,
            self.table_abc.fiz,
            self.table_efg.bam,
        )

        self.assertEqual('SELECT "abc"."baz","efg"."buz","abc"."fiz","efg"."bam" FROM "abc" '
                         'JOIN "efg" ON "abc"."foo"="efg"."bar"', str(q))

    def test_select__star_for_table(self):
        q = Query.from_(self.table_abc).join(self.table_efg).on(
            self.table_abc.foo == self.table_efg.bar
        ).join(self.table_hij).on(
            self.table_abc.buz == self.table_hij.bam
        ).select(self.table_abc.star).select(self.table_efg.star).select(self.table_hij.star)

        self.assertEqual('SELECT "abc".*,"efg".*,"hij".* FROM "abc" '
                         'JOIN "efg" ON "abc"."foo"="efg"."bar" '
                         'JOIN "hij" ON "abc"."buz"="hij"."bam"', str(q))

    def test_select__star_for_table__replacement(self):
        q = Query.from_(self.table_abc).join(self.table_efg).on(
            self.table_abc.foo == self.table_efg.bar
        ).join(self.table_hij).on(
            self.table_abc.buz == self.table_hij.bam
        ).select(
            self.table_abc.foo, self.table_efg.bar, self.table_hij.bam
        ).select(
            self.table_abc.star, self.table_efg.star, self.table_hij.star
        )

        self.assertEqual('SELECT "abc".*,"efg".*,"hij".* FROM "abc" '
                         'JOIN "efg" ON "abc"."foo"="efg"."bar" '
                         'JOIN "hij" ON "abc"."buz"="hij"."bam"', str(q))

    def test_select_fields_with_where(self):
        q = Query.from_(self.table_abc).join(
            self.table_efg).on(self.table_abc.foo == self.table_efg.bar
                               ).join(
            self.table_hij).on(self.table_abc.buz == self.table_hij.bam
                               ).select(
            self.table_abc.foo, self.table_efg.bar, self.table_hij.bam
        ).where(self.table_abc.foo > 1).where(self.table_efg.bar != 2)

        self.assertEqual('SELECT "abc"."foo","efg"."bar","hij"."bam" FROM "abc" '
                         'JOIN "efg" ON "abc"."foo"="efg"."bar" '
                         'JOIN "hij" ON "abc"."buz"="hij"."bam" '
                         'WHERE "abc"."foo">1 AND "efg"."bar"<>2', str(q))

    def test_require_condition(self):
        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).join(self.table_efg).on(None)

    def test_require_condition_with_both_tables(self):
        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).join(self.table_efg).on(self.table_abc.foo == self.table_hij.bar)

        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).join(self.table_efg).on(self.table_hij.foo == self.table_efg.bar)

        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).join(self.table_efg).on(self.table_hij.foo == self.table_klm.bar)

    def test_join_same_table(self):
        table1 = Table('abc')
        table2 = Table('abc')
        q = Query.from_(table1).join(table2).on(table1.foo == table2.bar).select(table1.foo, table2.buz)

        self.assertEqual('SELECT "abc"."foo","abc2"."buz" FROM "abc" '
                         'JOIN "abc" "abc2" ON "abc"."foo"="abc2"."bar"', str(q))

    def test_join_same_table_with_prefixes(self):
        table1 = Table('abc', alias='x')
        table2 = Table('abc', alias='y')
        q = Query.from_(table1).join(table2).on(table1.foo == table2.bar).select(table1.foo, table2.buz)

        self.assertEqual('SELECT "x"."foo","y"."buz" FROM "abc" "x" '
                         'JOIN "abc" "y" ON "x"."foo"="y"."bar"', str(q))

    def test_join_table_twice(self):
        table1, table2 = Table('efg', alias='efg1'), Table('efg', alias='efg2')
        q = Query.from_(self.table_abc) \
            .join(table1).on(self.table_abc.foo == table1.bar) \
            .join(table2).on(self.table_abc.foo == table2.bam) \
            .select(self.table_abc.foo, table1.fiz, table2.buz)

        self.assertEqual('SELECT "abc"."foo","efg1"."fiz","efg2"."buz" FROM "abc" '
                         'JOIN "efg" "efg1" ON "abc"."foo"="efg1"."bar" '
                         'JOIN "efg" "efg2" ON "abc"."foo"="efg2"."bam"', str(q))

    def test_select__fields_after_table_star(self):
        q = Query.from_(self.table_abc).join(self.table_efg).on(self.table_abc.foo == self.table_efg.bar).select(
            self.table_abc.star,
            self.table_efg.bar).select(
            self.table_abc.foo)

        self.assertEqual('SELECT "abc".*,"efg"."bar" FROM "abc" JOIN "efg" ON "abc"."foo"="efg"."bar"',
                         str(q))

    def test_fail_when_joining_unknown_type(self):
        with self.assertRaises(ValueError):
            Query.from_(self.table_abc).join('this is a string')

    def test_immutable__queries_after_join(self):
        query1 = Query.from_(self.table_abc).select(self.table_abc.foo)
        query2 = query1.join(self.table_efg).on(self.table_abc.foo == self.table_efg.bar).select(self.table_efg.buz)

        self.assertEqual('SELECT "foo" FROM "abc"', str(query1))
        self.assertEqual('SELECT "abc"."foo","efg"."buz" FROM "abc" '
                         'JOIN "efg" ON "abc"."foo"="efg"."bar"', str(query2))

    def test_immutable__tables(self):
        query1 = Query.from_(self.table_abc).select(self.table_abc.foo)
        query2 = Query.from_(self.table_abc).join(self.table_efg).on(self.table_abc.foo == self.table_efg.bar).select(
            self.table_abc.foo,
            self.table_efg.buz)

        self.assertEqual('SELECT "abc"."foo","efg"."buz" FROM "abc" '
                         'JOIN "efg" ON "abc"."foo"="efg"."bar"', str(query2))
        self.assertEqual('SELECT "foo" FROM "abc"', str(query1))

    def test_select_field_from_missing_table(self):
        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).select(self.table_efg.foo)

        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).where(self.table_efg.foo == 0)

        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).where(fn.Sum(self.table_efg.foo) == 0)

        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).select(fn.Sum(self.table_abc.bar * 2) + fn.Sum(self.table_efg.foo * 2))

        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).groupby(self.table_efg.foo)

        with self.assertRaises(JoinException):
            Query.from_(self.table_abc).groupby(self.table_abc.foo).having(self.table_efg.bar)

    def test_ignore_table_references(self):
        query = Query.from_(Table('abc')).select(Table('abc').foo)

        self.assertEqual('SELECT "foo" FROM "abc"', str(query))

    def test_prefixes_added_to_groupby(self):
        test_query = Query.from_(self.table_abc).join(self.table_efg).on(
            self.table_abc.foo == self.table_efg.bar
        ).select(self.table_abc.foo, fn.Sum(self.table_efg.buz)).groupby(self.table_abc.foo)

        self.assertEqual('SELECT "abc"."foo",SUM("efg"."buz") FROM "abc" '
                         'JOIN "efg" ON "abc"."foo"="efg"."bar" '
                         'GROUP BY "abc"."foo"', str(test_query))

    def test_prefixes_added_to_orderby(self):
        test_query = Query.from_(self.table_abc).join(self.table_efg).on(
            self.table_abc.foo == self.table_efg.bar
        ).select(self.table_abc.foo, self.table_efg.buz).orderby(self.table_abc.foo)

        self.assertEqual('SELECT "abc"."foo","efg"."buz" FROM "abc" '
                         'JOIN "efg" ON "abc"."foo"="efg"."bar" '
                         'ORDER BY "abc"."foo"', str(test_query))

    def test_prefixes_added_to_function_in_orderby(self):
        test_query = Query.from_(self.table_abc).join(self.table_efg).on(
            self.table_abc.foo == self.table_efg.bar
        ).select(self.table_abc.foo, self.table_efg.buz).orderby(fn.Date(self.table_abc.foo))

        self.assertEqual('SELECT "abc"."foo","efg"."buz" FROM "abc" '
                         'JOIN "efg" ON "abc"."foo"="efg"."bar" '
                         'ORDER BY DATE("abc"."foo")', str(test_query))

    def test_join_from_join(self):
        test_query = Query.from_(self.table_abc) \
            .join(self.table_efg) \
            .on(self.table_abc.efg_id == self.table_efg.id) \
            .join(self.table_hij) \
            .on(self.table_efg.hij_id == self.table_hij.id) \
            .select(self.table_abc.foo, self.table_efg.bar, self.table_hij.fizz)

        self.assertEqual('SELECT "abc"."foo","efg"."bar","hij"."fizz" FROM "abc" '
                         'JOIN "efg" ON "abc"."efg_id"="efg"."id" '
                         'JOIN "hij" ON "efg"."hij_id"="hij"."id"', str(test_query))


class UnionTests(unittest.TestCase):
    table1, table2 = Tables('abc', 'efg')

    def test_union(self):
        query1 = Query.from_(self.table1).select(self.table1.foo)
        query2 = Query.from_(self.table2).select(self.table2.bar)

        self.assertEqual('(SELECT "foo" FROM "abc") UNION (SELECT "bar" FROM "efg")', str(query1 + query2))
        self.assertEqual('(SELECT "foo" FROM "abc") UNION (SELECT "bar" FROM "efg")', str(query1.union(query2)))

    def test_union_all(self):
        query1 = Query.from_(self.table1).select(self.table1.foo)
        query2 = Query.from_(self.table2).select(self.table2.bar)

        self.assertEqual('(SELECT "foo" FROM "abc") UNION ALL (SELECT "bar" FROM "efg")', str(query1 * query2))
        self.assertEqual('(SELECT "foo" FROM "abc") UNION ALL (SELECT "bar" FROM "efg")', str(query1.union_all(query2)))

    def test_require_equal_number_of_fields(self):
        query1 = Query.from_(self.table1).select(self.table1.foo)
        query2 = Query.from_(self.table2).select(self.table2.fiz, self.table2.buz)

        with self.assertRaises(UnionException):
            str(query1 + query2)
