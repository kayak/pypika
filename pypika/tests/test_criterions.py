# coding: utf8
import unittest
from datetime import date, datetime

from pypika import Field, Table, functions as fn
from pypika.terms import Mod

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class CriterionTests(unittest.TestCase):
    t = Table('test', alias='crit')

    def test__criterion_with_alias(self):
        c1 = (Field('foo') == Field('bar')).as_('criterion')

        self.assertEqual('"foo"="bar"', str(c1))
        self.assertEqual('"foo"="bar" "criterion"', c1.get_sql(with_alias=True, quote_char='"'))

    def test__criterion_eq_number(self):
        c1 = Field('foo') == 1
        c2 = Field('foo', table=self.t).eq(0)
        c3 = Field('foo', table=self.t) == -1

        self.assertEqual('"foo"=1', str(c1))
        self.assertEqual('"crit"."foo"=0', str(c2))
        self.assertEqual('"crit"."foo"=-1', str(c3))

    def test__criterion_eq_decimal(self):
        c1 = Field('foo') == 1.0
        c2 = Field('foo', table=self.t).eq(0.5)

        self.assertEqual('"foo"=1.0', str(c1))
        self.assertEqual('"crit"."foo"=0.5', str(c2))

    def test__criterion_eq_bool(self):
        c1 = Field('foo') == True
        c2 = Field('foo', table=self.t).eq(True)
        c3 = Field('foo') == False
        c4 = Field('foo', table=self.t).eq(False)

        self.assertEqual('"foo"=true', str(c1))
        self.assertEqual('"crit"."foo"=true', str(c2))
        self.assertEqual('"foo"=false', str(c3))
        self.assertEqual('"crit"."foo"=false', str(c4))

    def test__criterion_eq_str(self):
        c1 = Field('foo') == 'abc'
        c2 = Field('foo', table=self.t).eq('abc')

        self.assertEqual('"foo"=\'abc\'', str(c1))
        self.assertEqual('"crit"."foo"=\'abc\'', str(c2))

    def test__criterion_eq_date(self):
        c1 = Field('foo') == date(2000, 1, 1)
        c2 = Field('foo', table=self.t).eq(date(2000, 1, 1))

        self.assertEqual('"foo"=\'2000-01-01\'', str(c1))
        self.assertEqual('"crit"."foo"=\'2000-01-01\'', str(c2))

    def test__criterion_eq_datetime(self):
        c1 = Field('foo') == datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).eq(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual('"foo"=\'2000-01-01T12:30:55\'', str(c1))
        self.assertEqual('"crit"."foo"=\'2000-01-01T12:30:55\'', str(c2))

    def test__criterion_eq_right(self):
        c1 = 1 == Field('foo')
        c2 = -1 == Field('foo', table=self.t)

        self.assertEqual('"foo"=1', str(c1))
        self.assertEqual('"crit"."foo"=-1', str(c2))

    def test__criterion_is_null(self):
        c1 = Field('foo').isnull()
        c2 = Field('foo', table=self.t).isnull()

        self.assertEqual('"foo" IS NULL', str(c1))
        self.assertEqual('"crit"."foo" IS NULL', str(c2))

    def test__criterion_ne_number(self):
        c1 = Field('foo') != 1
        c2 = Field('foo', table=self.t).ne(0)
        c3 = Field('foo') != -1

        self.assertEqual('"foo"<>1', str(c1))
        self.assertEqual('"crit"."foo"<>0', str(c2))
        self.assertEqual('"foo"<>-1', str(c3))

    def test__criterion_ne_str(self):
        c1 = Field('foo') != 'abc'
        c2 = Field('foo', table=self.t).ne('abc')

        self.assertEqual('"foo"<>\'abc\'', str(c1))
        self.assertEqual('"crit"."foo"<>\'abc\'', str(c2))

    def test__criterion_ne_date(self):
        c1 = Field('foo') != date(2000, 1, 1)
        c2 = Field('foo', table=self.t).ne(date(2000, 1, 1))

        self.assertEqual('"foo"<>\'2000-01-01\'', str(c1))
        self.assertEqual('"crit"."foo"<>\'2000-01-01\'', str(c2))

    def test__criterion_ne_datetime(self):
        c1 = Field('foo') != datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).ne(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual('"foo"<>\'2000-01-01T12:30:55\'', str(c1))
        self.assertEqual('"crit"."foo"<>\'2000-01-01T12:30:55\'', str(c2))

    def test__criterion_ne_right(self):
        c1 = 1 != Field('foo')
        c2 = -1 != Field('foo', table=self.t)

        self.assertEqual('"foo"<>1', str(c1))
        self.assertEqual('"crit"."foo"<>-1', str(c2))

    def test__criterion_lt_number(self):
        c1 = Field('foo') < 1
        c2 = Field('foo', table=self.t).lt(0)
        c3 = Field('foo') < -1

        self.assertEqual('"foo"<1', str(c1))
        self.assertEqual('"crit"."foo"<0', str(c2))
        self.assertEqual('"foo"<-1', str(c3))

    def test__criterion_lt_date(self):
        c1 = Field('foo') < date(2000, 1, 1)
        c2 = Field('foo', table=self.t).lt(date(2000, 1, 1))

        self.assertEqual('"foo"<\'2000-01-01\'', str(c1))
        self.assertEqual('"crit"."foo"<\'2000-01-01\'', str(c2))

    def test__criterion_lt_datetime(self):
        c1 = Field('foo') < datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).lt(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual('"foo"<\'2000-01-01T12:30:55\'', str(c1))
        self.assertEqual('"crit"."foo"<\'2000-01-01T12:30:55\'', str(c2))

    def test__criterion_lt_right(self):
        c1 = 1 > Field('foo')
        c2 = -1 > Field('foo', table=self.t)

        self.assertEqual('"foo"<1', str(c1))
        self.assertEqual('"crit"."foo"<-1', str(c2))

    def test__criterion_gt_number(self):
        c1 = Field('foo') > 1
        c2 = Field('foo', table=self.t).gt(0)
        c3 = Field('foo') > -1

        self.assertEqual('"foo">1', str(c1))
        self.assertEqual('"crit"."foo">0', str(c2))
        self.assertEqual('"foo">-1', str(c3))

    def test__criterion_gt_date(self):
        c1 = Field('foo') > date(2000, 1, 1)
        c2 = Field('foo', table=self.t).gt(date(2000, 1, 1))

        self.assertEqual('"foo">\'2000-01-01\'', str(c1))
        self.assertEqual('"crit"."foo">\'2000-01-01\'', str(c2))

    def test__criterion_gt_datetime(self):
        c1 = Field('foo') > datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).gt(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual('"foo">\'2000-01-01T12:30:55\'', str(c1))
        self.assertEqual('"crit"."foo">\'2000-01-01T12:30:55\'', str(c2))

    def test__criterion_gt_right(self):
        c1 = 1 < Field('foo')
        c2 = -1 < Field('foo', table=self.t)

        self.assertEqual('"foo">1', str(c1))
        self.assertEqual('"crit"."foo">-1', str(c2))

    def test__criterion_lte_number(self):
        c1 = Field('foo') <= 1
        c2 = Field('foo', table=self.t).lte(0)
        c3 = Field('foo') <= -1

        self.assertEqual('"foo"<=1', str(c1))
        self.assertEqual('"crit"."foo"<=0', str(c2))
        self.assertEqual('"foo"<=-1', str(c3))

    def test__criterion_lte_date(self):
        c1 = Field('foo') <= date(2000, 1, 1)
        c2 = Field('foo', table=self.t).lte(date(2000, 1, 1))

        self.assertEqual('"foo"<=\'2000-01-01\'', str(c1))
        self.assertEqual('"crit"."foo"<=\'2000-01-01\'', str(c2))

    def test__criterion_lte_datetime(self):
        c1 = Field('foo') <= datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).lte(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual('"foo"<=\'2000-01-01T12:30:55\'', str(c1))
        self.assertEqual('"crit"."foo"<=\'2000-01-01T12:30:55\'', str(c2))

    def test__criterion_lte_right(self):
        c1 = 1 >= Field('foo')
        c2 = -1 >= Field('foo', table=self.t)

        self.assertEqual('"foo"<=1', str(c1))
        self.assertEqual('"crit"."foo"<=-1', str(c2))

    def test__criterion_gte_number(self):
        c1 = Field('foo') >= 1
        c2 = Field('foo', table=self.t).gte(0)
        c3 = Field('foo') >= -1

        self.assertEqual('"foo">=1', str(c1))
        self.assertEqual('"crit"."foo">=0', str(c2))
        self.assertEqual('"foo">=-1', str(c3))

    def test__criterion_gte_date(self):
        c1 = Field('foo') >= date(2000, 1, 1)
        c2 = Field('foo', table=self.t).gte(date(2000, 1, 1))

        self.assertEqual('"foo">=\'2000-01-01\'', str(c1))
        self.assertEqual('"crit"."foo">=\'2000-01-01\'', str(c2))

    def test__criterion_gte_datetime(self):
        c1 = Field('foo') >= datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).gte(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual('"foo">=\'2000-01-01T12:30:55\'', str(c1))
        self.assertEqual('"crit"."foo">=\'2000-01-01T12:30:55\'', str(c2))

    def test__criterion_gte_right(self):
        c1 = 1 <= Field('foo')
        c2 = -1 <= Field('foo', table=self.t)

        self.assertEqual('"foo">=1', str(c1))
        self.assertEqual('"crit"."foo">=-1', str(c2))


class NotTests(unittest.TestCase):
    table_abc = Table('abc', alias='cx0')

    def test_negate(self):
        c1 = Field('foo') >= 1
        c2 = c1.negate()

        self.assertEqual('"foo">=1', str(c1))
        self.assertEqual('NOT "foo">=1', str(c2))

    def test_variable_access(self):
        c1 = Field('foo').negate()

        self.assertEqual(c1.is_aggregate, False)

    def test_chained_function(self):
        field1 = Field('foo').negate()
        field2 = field1.eq('bar')

        self.assertEqual('NOT "foo"', str(field1))
        self.assertEqual('NOT "foo"=\'bar\'', str(field2))
        self.assertIsNot(field1, field2)

    def test_not_null(self):
        c1 = Field('foo').notnull()
        c2 = Field('foo', table=self.table_abc).notnull()

        self.assertEqual('NOT "foo" IS NULL', str(c1))
        self.assertEqual('NOT "cx0"."foo" IS NULL', str(c2))

    def test_notnullcriterion_for_table(self):
        f = self.table_abc.foo.notnull().for_(self.table_abc)

        self.assertEqual('NOT "cx0"."foo" IS NULL', str(f))


class BetweenTests(unittest.TestCase):
    t = Table('abc', alias='btw')

    def test__between_number(self):
        c1 = Field('foo').between(0, 1)
        c2 = Field('foo', table=self.t).between(0, 1)
        c3 = Field('foo')[0:1]

        self.assertEqual('"foo" BETWEEN 0 AND 1', str(c1))
        self.assertEqual('"btw"."foo" BETWEEN 0 AND 1', str(c2))
        self.assertEqual('"foo" BETWEEN 0 AND 1', str(c3))

    def test__between_date(self):
        c1 = Field('foo').between(date(2000, 1, 1), date(2000, 12, 31))
        c2 = Field('foo', table=self.t).between(date(2000, 1, 1), date(2000, 12, 31))
        c3 = Field('foo')[date(2000, 1, 1):date(2000, 12, 31)]

        self.assertEqual('"foo" BETWEEN \'2000-01-01\' AND \'2000-12-31\'', str(c1))
        self.assertEqual('"btw"."foo" BETWEEN \'2000-01-01\' AND \'2000-12-31\'', str(c2))
        self.assertEqual('"foo" BETWEEN \'2000-01-01\' AND \'2000-12-31\'', str(c3))

    def test__between_datetime(self):
        c1 = Field('foo').between(datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59))
        c2 = Field('foo', table=self.t).between(datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59))
        c3 = Field('foo')[datetime(2000, 1, 1, 0, 0, 0):datetime(2000, 12, 31, 23, 59, 59)]

        self.assertEqual('"foo" BETWEEN \'2000-01-01T00:00:00\' AND \'2000-12-31T23:59:59\'', str(c1))
        self.assertEqual('"btw"."foo" BETWEEN \'2000-01-01T00:00:00\' AND \'2000-12-31T23:59:59\'', str(c2))
        self.assertEqual('"foo" BETWEEN \'2000-01-01T00:00:00\' AND \'2000-12-31T23:59:59\'', str(c3))

    def test__function_between(self):
        c1 = fn.Coalesce(Field('foo'), 0)[0:1]
        c2 = fn.Coalesce(Field('foo', table=self.t), 0)[0:1]

        self.assertEqual('COALESCE("foo",0) BETWEEN 0 AND 1', str(c1))
        self.assertEqual('COALESCE("btw"."foo",0) BETWEEN 0 AND 1', str(c2))

    def test_get_item_only_works_with_slice(self):
        with self.assertRaises(TypeError):
            Field('foo')[0]

        with self.assertRaises(TypeError):
            Field('foo')[date(2000, 1, 1)]

        with self.assertRaises(TypeError):
            Field('foo')[datetime(2000, 1, 1, 0, 0, 0)]


class IsInTests(unittest.TestCase):
    t = Table('abc', alias='isin')

    def test__in_number(self):
        c1 = Field('foo').isin([0, 1])
        c2 = Field('foo', table=self.t).isin([0, 1])

        self.assertEqual('"foo" IN (0,1)', str(c1))
        self.assertEqual('"isin"."foo" IN (0,1)', str(c2))

    def test__in_character(self):
        c1 = Field('foo').isin(['a', 'b'])
        c2 = Field('foo', table=self.t).isin(['a', 'b'])

        self.assertEqual('"foo" IN (\'a\',\'b\')', str(c1))
        self.assertEqual('"isin"."foo" IN (\'a\',\'b\')', str(c2))

    def test__in_date(self):
        c1 = Field('foo').isin([date(2000, 1, 1), date(2000, 12, 31)])
        c2 = Field('foo', table=self.t).isin([date(2000, 1, 1), date(2000, 12, 31)])

        self.assertEqual('"foo" IN (\'2000-01-01\',\'2000-12-31\')', str(c1))
        self.assertEqual('"isin"."foo" IN (\'2000-01-01\',\'2000-12-31\')', str(c2))

    def test__in_datetime(self):
        c1 = Field('foo').isin([datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59)])
        c2 = Field('foo', table=self.t).isin([datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59)])

        self.assertEqual('"foo" IN (\'2000-01-01T00:00:00\',\'2000-12-31T23:59:59\')', str(c1))
        self.assertEqual('"isin"."foo" IN (\'2000-01-01T00:00:00\',\'2000-12-31T23:59:59\')', str(c2))

    def test__function_isin(self):
        c1 = fn.Coalesce(Field('foo'), 0).isin([0, 1])
        c2 = fn.Coalesce(Field('foo', table=self.t), 0).isin([0, 1])

        self.assertEqual('COALESCE("foo",0) IN (0,1)', str(c1))
        self.assertEqual('COALESCE("isin"."foo",0) IN (0,1)', str(c2))


class NotInTests(unittest.TestCase):
    t = Table('abc', alias='notin')

    def test__notin_number(self):
        c1 = Field('foo').notin([0, 1])
        c2 = Field('foo', table=self.t).notin([0, 1])

        self.assertEqual('"foo" NOT IN (0,1)', str(c1))
        self.assertEqual('"notin"."foo" NOT IN (0,1)', str(c2))

    def test__notin_character(self):
        c1 = Field('foo').notin(['a', 'b'])
        c2 = Field('foo', table=self.t).notin(['a', 'b'])

        self.assertEqual('"foo" NOT IN (\'a\',\'b\')', str(c1))
        self.assertEqual('"notin"."foo" NOT IN (\'a\',\'b\')', str(c2))

    def test__notin_date(self):
        c1 = Field('foo').notin([date(2000, 1, 1), date(2000, 12, 31)])
        c2 = Field('foo', table=self.t).notin([date(2000, 1, 1), date(2000, 12, 31)])

        self.assertEqual('"foo" NOT IN (\'2000-01-01\',\'2000-12-31\')', str(c1))
        self.assertEqual('"notin"."foo" NOT IN (\'2000-01-01\',\'2000-12-31\')', str(c2))

    def test__notin_datetime(self):
        c1 = Field('foo').notin([datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59)])
        c2 = Field('foo', table=self.t).notin([datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59)])

        self.assertEqual('"foo" NOT IN (\'2000-01-01T00:00:00\',\'2000-12-31T23:59:59\')', str(c1))
        self.assertEqual('"notin"."foo" NOT IN (\'2000-01-01T00:00:00\',\'2000-12-31T23:59:59\')', str(c2))

    def test__function_notin(self):
        c1 = fn.Coalesce(Field('foo'), 0).notin([0, 1])
        c2 = fn.Coalesce(Field('foo', table=self.t), 0).notin([0, 1])

        self.assertEqual('COALESCE("foo",0) NOT IN (0,1)', str(c1))
        self.assertEqual('COALESCE("notin"."foo",0) NOT IN (0,1)', str(c2))


class ComplexCriterionTests(unittest.TestCase):
    table_abc, table_efg = Table('abc', alias='cx0'), Table('efg', alias='cx1')

    def test__and(self):
        c1 = (Field('foo') == 1) & (Field('bar') == 2)
        c2 = (Field('foo', table=self.table_abc) == 1) & (Field('bar', table=self.table_efg) == 2)

        self.assertEqual('"foo"=1 AND "bar"=2', str(c1))
        self.assertEqual('"cx0"."foo"=1 AND "cx1"."bar"=2', str(c2))

    def test__or(self):
        c1 = (Field('foo') == 1) | (Field('bar') == 2)
        c2 = (Field('foo', table=self.table_abc) == 1) | (Field('bar', table=self.table_efg) == 2)

        self.assertEqual('"foo"=1 OR "bar"=2', str(c1))
        self.assertEqual('"cx0"."foo"=1 OR "cx1"."bar"=2', str(c2))

    def test__xor(self):
        c1 = (Field('foo') == 1) ^ (Field('bar') == 2)
        c2 = (Field('foo', table=self.table_abc) == 1) ^ (Field('bar', table=self.table_efg) == 2)

        self.assertEqual('"foo"=1 XOR "bar"=2', str(c1))
        self.assertEqual('"cx0"."foo"=1 XOR "cx1"."bar"=2', str(c2))

    def test__nested__and(self):
        c = (Field('foo') == 1) & (Field('bar') == 2) & (Field('buz') == 3)

        self.assertEqual('"foo"=1 AND "bar"=2 AND "buz"=3', str(c))

    def test__nested__or(self):
        c = (Field('foo') == 1) | (Field('bar') == 2) | (Field('buz') == 3)

        self.assertEqual('"foo"=1 OR "bar"=2 OR "buz"=3', str(c))

    def test__nested__mixed(self):
        c = ((Field('foo') == 1) & (Field('bar') == 2)) | (Field('buz') == 3)

        self.assertEqual('("foo"=1 AND "bar"=2) OR "buz"=3', str(c))

    def test__between_and_isin(self):
        c = Field('foo').isin([1, 2, 3]) & Field('bar').between(0, 1)

        self.assertEqual('"foo" IN (1,2,3) AND "bar" BETWEEN 0 AND 1', str(c))


class CriterionOperationsTests(unittest.TestCase):
    table_abc, table_efg = Table('abc', alias='cx0'), Table('efg', alias='cx1')

    def test_field_for_table(self):
        f = self.table_abc.foo.for_(self.table_efg)

        self.assertEqual('"cx1"."foo"', str(f))

    def test_arithmeticfunction_for_table(self):
        f = (self.table_abc.foo + self.table_abc.bar).for_(self.table_efg)

        self.assertEqual('"cx1"."foo"+"cx1"."bar"', str(f))

    def test_criterion_for_table(self):
        f = (self.table_abc.foo < self.table_abc.bar).for_(self.table_efg)

        self.assertEqual('"cx1"."foo"<"cx1"."bar"', str(f))

    def test_complexcriterion_for_table(self):
        f = ((self.table_abc.foo < self.table_abc.bar) & (self.table_abc.fiz > self.table_abc.buz)).for_(self.table_efg)

        self.assertEqual('"cx1"."foo"<"cx1"."bar" AND "cx1"."fiz">"cx1"."buz"', str(f))

    def test_function_with_only_fields_for_table(self):
        f = fn.Sum(self.
                   table_abc.foo).for_(self.table_efg)

        self.assertEqual('SUM("cx1"."foo")', str(f))

    def test_function_with_values_and_fields_for_table(self):
        f = Mod(self.table_abc.foo, 2).for_(self.table_efg)

        self.assertEqual('MOD("cx1"."foo",2)', str(f))

    def test_betweencriterion_for_table(self):
        f = self.table_abc.foo[0:1].for_(self.table_efg)

        self.assertEqual('"cx1"."foo" BETWEEN 0 AND 1', str(f))

    def test_nullcriterion_for_table(self):
        f = self.table_abc.foo.isnull().for_(self.table_efg)

        self.assertEqual('"cx1"."foo" IS NULL', str(f))
