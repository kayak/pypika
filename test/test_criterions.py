# coding: utf8
import unittest
from datetime import date, datetime

from pypika import Field, Table

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"
__version__ = "0.0.1"


class CriterionTests(unittest.TestCase):
    t = Table('test')
    t._alias = 't0'

    def test__criterion_eq_number(self):
        c1 = Field('foo') == 1
        c2 = Field('foo', table=self.t).eq(0)
        c3 = Field('foo', table=self.t) == -1

        self.assertEqual('foo=1', str(c1))
        self.assertEqual('t0.foo=0', str(c2))
        self.assertEqual('t0.foo=-1', str(c3))

    def test__criterion_eq_decimal(self):
        c1 = Field('foo') == 1.0
        c2 = Field('foo', table=self.t).eq(0.5)

        self.assertEqual('foo=1.0', str(c1))
        self.assertEqual('t0.foo=0.5', str(c2))

    def test__criterion_eq_bool(self):
        c1 = Field('foo') == True
        c2 = Field('foo', table=self.t).eq(True)
        c3 = Field('foo') == False
        c4 = Field('foo', table=self.t).eq(False)

        self.assertEqual('foo=true', str(c1))
        self.assertEqual('t0.foo=true', str(c2))
        self.assertEqual('foo=false', str(c3))
        self.assertEqual('t0.foo=false', str(c4))

    def test__criterion_eq_str(self):
        c1 = Field('foo') == 'abc'
        c2 = Field('foo', table=self.t).eq('abc')

        self.assertEqual("foo='abc'", str(c1))
        self.assertEqual("t0.foo='abc'", str(c2))

    def test__criterion_eq_date(self):
        c1 = Field('foo') == date(2000, 1, 1)
        c2 = Field('foo', table=self.t).eq(date(2000, 1, 1))

        self.assertEqual("foo='2000-01-01'", str(c1))
        self.assertEqual("t0.foo='2000-01-01'", str(c2))

    def test__criterion_eq_datetime(self):
        c1 = Field('foo') == datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).eq(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual("foo='2000-01-01T12:30:55'", str(c1))
        self.assertEqual("t0.foo='2000-01-01T12:30:55'", str(c2))

    def test__criterion_eq_right(self):
        c1 = 1 == Field('foo')
        c2 = -1 == Field('foo', table=self.t)

        self.assertEqual('foo=1', str(c1))
        self.assertEqual('t0.foo=-1', str(c2))

    def test__criterion_is_null(self):
        c1 = Field('foo').isnull()
        c2 = Field('foo', table=self.t).isnull()

        self.assertEqual('foo IS NULL', str(c1))
        self.assertEqual('t0.foo IS NULL', str(c2))

    def test__criterion_is_not_null(self):
        c1 = Field('foo').notnull()
        c2 = Field('foo', table=self.t).notnull()

        self.assertEqual('foo IS NOT NULL', str(c1))
        self.assertEqual('t0.foo IS NOT NULL', str(c2))

    def test__criterion_ne_number(self):
        c1 = Field('foo') != 1
        c2 = Field('foo', table=self.t).ne(0)
        c3 = Field('foo') != -1

        self.assertEqual('foo<>1', str(c1))
        self.assertEqual('t0.foo<>0', str(c2))
        self.assertEqual('foo<>-1', str(c3))

    def test__criterion_ne_str(self):
        c1 = Field('foo') != 'abc'
        c2 = Field('foo', table=self.t).ne('abc')

        self.assertEqual("foo<>'abc'", str(c1))
        self.assertEqual("t0.foo<>'abc'", str(c2))

    def test__criterion_ne_date(self):
        c1 = Field('foo') != date(2000, 1, 1)
        c2 = Field('foo', table=self.t).ne(date(2000, 1, 1))

        self.assertEqual("foo<>'2000-01-01'", str(c1))
        self.assertEqual("t0.foo<>'2000-01-01'", str(c2))

    def test__criterion_ne_datetime(self):
        c1 = Field('foo') != datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).ne(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual("foo<>'2000-01-01T12:30:55'", str(c1))
        self.assertEqual("t0.foo<>'2000-01-01T12:30:55'", str(c2))

    def test__criterion_ne_right(self):
        c1 = 1 != Field('foo')
        c2 = -1 != Field('foo', table=self.t)

        self.assertEqual('foo<>1', str(c1))
        self.assertEqual('t0.foo<>-1', str(c2))

    def test__criterion_lt_number(self):
        c1 = Field('foo') < 1
        c2 = Field('foo', table=self.t).lt(0)
        c3 = Field('foo') < -1

        self.assertEqual('foo<1', str(c1))
        self.assertEqual('t0.foo<0', str(c2))
        self.assertEqual('foo<-1', str(c3))

    def test__criterion_lt_date(self):
        c1 = Field('foo') < date(2000, 1, 1)
        c2 = Field('foo', table=self.t).lt(date(2000, 1, 1))

        self.assertEqual("foo<'2000-01-01'", str(c1))
        self.assertEqual("t0.foo<'2000-01-01'", str(c2))

    def test__criterion_lt_datetime(self):
        c1 = Field('foo') < datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).lt(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual("foo<'2000-01-01T12:30:55'", str(c1))
        self.assertEqual("t0.foo<'2000-01-01T12:30:55'", str(c2))

    def test__criterion_lt_right(self):
        c1 = 1 > Field('foo')
        c2 = -1 > Field('foo', table=self.t)

        self.assertEqual('foo<1', str(c1))
        self.assertEqual('t0.foo<-1', str(c2))

    def test__criterion_gt_number(self):
        c1 = Field('foo') > 1
        c2 = Field('foo', table=self.t).gt(0)
        c3 = Field('foo') > -1

        self.assertEqual('foo>1', str(c1))
        self.assertEqual('t0.foo>0', str(c2))
        self.assertEqual('foo>-1', str(c3))

    def test__criterion_gt_date(self):
        c1 = Field('foo') > date(2000, 1, 1)
        c2 = Field('foo', table=self.t).gt(date(2000, 1, 1))

        self.assertEqual("foo>'2000-01-01'", str(c1))
        self.assertEqual("t0.foo>'2000-01-01'", str(c2))

    def test__criterion_gt_datetime(self):
        c1 = Field('foo') > datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).gt(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual("foo>'2000-01-01T12:30:55'", str(c1))
        self.assertEqual("t0.foo>'2000-01-01T12:30:55'", str(c2))

    def test__criterion_gt_right(self):
        c1 = 1 < Field('foo')
        c2 = -1 < Field('foo', table=self.t)

        self.assertEqual('foo>1', str(c1))
        self.assertEqual('t0.foo>-1', str(c2))

    def test__criterion_lte_number(self):
        c1 = Field('foo') <= 1
        c2 = Field('foo', table=self.t).lte(0)
        c3 = Field('foo') <= -1

        self.assertEqual('foo<=1', str(c1))
        self.assertEqual('t0.foo<=0', str(c2))
        self.assertEqual('foo<=-1', str(c3))

    def test__criterion_lte_date(self):
        c1 = Field('foo') <= date(2000, 1, 1)
        c2 = Field('foo', table=self.t).lte(date(2000, 1, 1))

        self.assertEqual("foo<='2000-01-01'", str(c1))
        self.assertEqual("t0.foo<='2000-01-01'", str(c2))

    def test__criterion_lte_datetime(self):
        c1 = Field('foo') <= datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).lte(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual("foo<='2000-01-01T12:30:55'", str(c1))
        self.assertEqual("t0.foo<='2000-01-01T12:30:55'", str(c2))

    def test__criterion_lte_right(self):
        c1 = 1 >= Field('foo')
        c2 = -1 >= Field('foo', table=self.t)

        self.assertEqual('foo<=1', str(c1))
        self.assertEqual('t0.foo<=-1', str(c2))

    def test__criterion_gte_number(self):
        c1 = Field('foo') >= 1
        c2 = Field('foo', table=self.t).gte(0)
        c3 = Field('foo') >= -1

        self.assertEqual('foo>=1', str(c1))
        self.assertEqual('t0.foo>=0', str(c2))
        self.assertEqual('foo>=-1', str(c3))

    def test__criterion_gte_date(self):
        c1 = Field('foo') >= date(2000, 1, 1)
        c2 = Field('foo', table=self.t).gte(date(2000, 1, 1))

        self.assertEqual("foo>='2000-01-01'", str(c1))
        self.assertEqual("t0.foo>='2000-01-01'", str(c2))

    def test__criterion_gte_datetime(self):
        c1 = Field('foo') >= datetime(2000, 1, 1, 12, 30, 55)
        c2 = Field('foo', table=self.t).gte(datetime(2000, 1, 1, 12, 30, 55))

        self.assertEqual("foo>='2000-01-01T12:30:55'", str(c1))
        self.assertEqual("t0.foo>='2000-01-01T12:30:55'", str(c2))

    def test__criterion_gte_right(self):
        c1 = 1 <= Field('foo')
        c2 = -1 <= Field('foo', table=self.t)

        self.assertEqual('foo>=1', str(c1))
        self.assertEqual('t0.foo>=-1', str(c2))


class BetweenTests(unittest.TestCase):
    t = Table('abc')
    t._alias = 't0'

    def test__between_number(self):
        c1 = Field('foo').between(0, 1)
        c2 = Field('foo', table=self.t).between(0, 1)
        c3 = Field('foo')[0:1]

        self.assertEqual('foo BETWEEN 0 AND 1', str(c1))
        self.assertEqual('t0.foo BETWEEN 0 AND 1', str(c2))
        self.assertEqual('foo BETWEEN 0 AND 1', str(c3))

    def test__between_date(self):
        c1 = Field('foo').between(date(2000, 1, 1), date(2000, 12, 31))
        c2 = Field('foo', table=self.t).between(date(2000, 1, 1), date(2000, 12, 31))
        c3 = Field('foo')[date(2000, 1, 1):date(2000, 12, 31)]

        self.assertEqual("foo BETWEEN '2000-01-01' AND '2000-12-31'", str(c1))
        self.assertEqual("t0.foo BETWEEN '2000-01-01' AND '2000-12-31'", str(c2))
        self.assertEqual("foo BETWEEN '2000-01-01' AND '2000-12-31'", str(c3))

    def test__between_datetime(self):
        c1 = Field('foo').between(datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59))
        c2 = Field('foo', table=self.t).between(datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59))
        c3 = Field('foo')[datetime(2000, 1, 1, 0, 0, 0):datetime(2000, 12, 31, 23, 59, 59)]

        self.assertEqual("foo BETWEEN '2000-01-01T00:00:00' AND '2000-12-31T23:59:59'", str(c1))
        self.assertEqual("t0.foo BETWEEN '2000-01-01T00:00:00' AND '2000-12-31T23:59:59'", str(c2))
        self.assertEqual("foo BETWEEN '2000-01-01T00:00:00' AND '2000-12-31T23:59:59'", str(c3))

    def test_get_item_only_works_with_slice(self):
        f = None

        with self.assertRaises(TypeError):
            f = Field('foo')[0]

        with self.assertRaises(TypeError):
            f = Field('foo')[date(2000, 1, 1)]

        with self.assertRaises(TypeError):
            f = Field('foo')[datetime(2000, 1, 1, 0, 0, 0)]

        self.assertIsNone(f)


class IsInTests(unittest.TestCase):
    t = Table('abc')
    t._alias = 't0'

    def test__between_number(self):
        c1 = Field('foo').isin([0, 1])
        c2 = Field('foo', table=self.t).isin([0, 1])

        self.assertEqual('foo IN (0,1)', str(c1))
        self.assertEqual('t0.foo IN (0,1)', str(c2))

    def test__between_date(self):
        c1 = Field('foo').isin([date(2000, 1, 1), date(2000, 12, 31)])
        c2 = Field('foo', table=self.t).isin([date(2000, 1, 1), date(2000, 12, 31)])

        self.assertEqual("foo IN ('2000-01-01','2000-12-31')", str(c1))
        self.assertEqual("t0.foo IN ('2000-01-01','2000-12-31')", str(c2))

    def test__between_datetime(self):
        c1 = Field('foo').isin([datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59)])
        c2 = Field('foo', table=self.t).isin([datetime(2000, 1, 1, 0, 0, 0), datetime(2000, 12, 31, 23, 59, 59)])

        self.assertEqual("foo IN ('2000-01-01T00:00:00','2000-12-31T23:59:59')", str(c1))
        self.assertEqual("t0.foo IN ('2000-01-01T00:00:00','2000-12-31T23:59:59')", str(c2))


class ComplexCriterionTests(unittest.TestCase):
    t0, t1 = Table('abc'), Table('efg')
    t0._alias, t1._alias = 't0', 't1'

    def test__and(self):
        c1 = (Field('foo') == 1) & (Field('bar') == 2)
        c2 = (Field('foo', table=self.t0) == 1) & (Field('bar', table=self.t1) == 2)

        self.assertEqual('foo=1 AND bar=2', str(c1))
        self.assertEqual('t0.foo=1 AND t1.bar=2', str(c2))

    def test__or(self):
        c1 = (Field('foo') == 1) | (Field('bar') == 2)
        c2 = (Field('foo', table=self.t0) == 1) | (Field('bar', table=self.t1) == 2)

        self.assertEqual('foo=1 OR bar=2', str(c1))
        self.assertEqual('t0.foo=1 OR t1.bar=2', str(c2))

    def test__xor(self):
        c1 = (Field('foo') == 1) ^ (Field('bar') == 2)
        c2 = (Field('foo', table=self.t0) == 1) ^ (Field('bar', table=self.t1) == 2)

        self.assertEqual('foo=1 XOR bar=2', str(c1))
        self.assertEqual('t0.foo=1 XOR t1.bar=2', str(c2))

    def test__nested__and(self):
        c = (Field('foo') == 1) & (Field('bar') == 2) & (Field('buz') == 3)

        self.assertEqual('foo=1 AND bar=2 AND buz=3', str(c))

    def test__nested__or(self):
        c = (Field('foo') == 1) | (Field('bar') == 2) | (Field('buz') == 3)

        self.assertEqual('foo=1 OR bar=2 OR buz=3', str(c))

    def test__nested__mixed(self):
        c = ((Field('foo') == 1) & (Field('bar') == 2)) | (Field('buz') == 3)

        self.assertEqual('(foo=1 AND bar=2) OR buz=3', str(c))

    def test__between_and_isin(self):
        c = Field('foo').isin([1, 2, 3]) & Field('bar').between(0, 1)

        self.assertEqual('foo IN (1,2,3) AND bar BETWEEN 0 AND 1', str(c))
