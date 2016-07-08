# coding: utf8
import unittest

from pypika import Query, F, Table, fn, CaseException, Case, Interval, DatePart

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"
__version__ = "0.0.1"


class ArithmeticTests(unittest.TestCase):
    t = Table('abc')

    def test__addition__fields(self):
        q1 = Query.from_('abc').select(F('a') + F('b'))
        q2 = Query.from_(self.t).select(self.t.a + self.t.b)

        self.assertEqual('SELECT a+b FROM abc', str(q1))
        self.assertEqual('SELECT a+b FROM abc', str(q2))

    def test__addition__number(self):
        q1 = Query.from_('abc').select(F('a') + 1)
        q2 = Query.from_(self.t).select(self.t.a + 1)

        self.assertEqual('SELECT a+1 FROM abc', str(q1))
        self.assertEqual('SELECT a+1 FROM abc', str(q2))

    def test__addition__decimal(self):
        q1 = Query.from_('abc').select(F('a') + 1.0)
        q2 = Query.from_(self.t).select(self.t.a + 1.0)

        self.assertEqual('SELECT a+1.0 FROM abc', str(q1))
        self.assertEqual('SELECT a+1.0 FROM abc', str(q2))

    def test__addition__right(self):
        q1 = Query.from_('abc').select(1 + F('a'))
        q2 = Query.from_(self.t).select(1 + self.t.a)

        self.assertEqual('SELECT 1+a FROM abc', str(q1))
        self.assertEqual('SELECT 1+a FROM abc', str(q2))

    def test__subtraction__fields(self):
        q1 = Query.from_('abc').select(F('a') - F('b'))
        q2 = Query.from_(self.t).select(self.t.a - self.t.b)

        self.assertEqual('SELECT a-b FROM abc', str(q1))
        self.assertEqual('SELECT a-b FROM abc', str(q2))

    def test__subtraction__number(self):
        q1 = Query.from_('abc').select(F('a') - 1)
        q2 = Query.from_(self.t).select(self.t.a - 1)

        self.assertEqual('SELECT a-1 FROM abc', str(q1))
        self.assertEqual('SELECT a-1 FROM abc', str(q2))

    def test__subtraction__decimal(self):
        q1 = Query.from_('abc').select(F('a') - 1.0)
        q2 = Query.from_(self.t).select(self.t.a - 1.0)

        self.assertEqual('SELECT a-1.0 FROM abc', str(q1))
        self.assertEqual('SELECT a-1.0 FROM abc', str(q2))

    def test__subtraction__right(self):
        q1 = Query.from_('abc').select(1 - F('a'))
        q2 = Query.from_(self.t).select(1 - self.t.a)

        self.assertEqual('SELECT 1-a FROM abc', str(q1))

    def test__multiplication__fields(self):
        q1 = Query.from_('abc').select(F('a') * F('b'))
        q2 = Query.from_(self.t).select(self.t.a * self.t.b)

        self.assertEqual('SELECT a*b FROM abc', str(q1))
        self.assertEqual('SELECT a*b FROM abc', str(q2))

    def test__multiplication__number(self):
        q1 = Query.from_('abc').select(F('a') * 1)
        q2 = Query.from_(self.t).select(self.t.a * 1)

        self.assertEqual('SELECT a*1 FROM abc', str(q1))
        self.assertEqual('SELECT a*1 FROM abc', str(q2))

    def test__multiplication__decimal(self):
        q1 = Query.from_('abc').select(F('a') * 1.0)
        q2 = Query.from_(self.t).select(self.t.a * 1.0)

        self.assertEqual('SELECT a*1.0 FROM abc', str(q1))
        self.assertEqual('SELECT a*1.0 FROM abc', str(q2))

    def test__multiplication__right(self):
        q1 = Query.from_('abc').select(1 * F('a'))
        q2 = Query.from_(self.t).select(1 * self.t.a)

        self.assertEqual('SELECT 1*a FROM abc', str(q1))
        self.assertEqual('SELECT 1*a FROM abc', str(q2))

    def test__division__fields(self):
        q1 = Query.from_('abc').select(F('a') / F('b'))
        q2 = Query.from_(self.t).select(self.t.a / self.t.b)

        self.assertEqual('SELECT a/b FROM abc', str(q1))
        self.assertEqual('SELECT a/b FROM abc', str(q2))

    def test__division__number(self):
        q1 = Query.from_('abc').select(F('a') / 1)
        q2 = Query.from_(self.t).select(self.t.a / 1)

        self.assertEqual('SELECT a/1 FROM abc', str(q1))
        self.assertEqual('SELECT a/1 FROM abc', str(q2))

    def test__division__decimal(self):
        q1 = Query.from_('abc').select(F('a') / 1.0)
        q2 = Query.from_(self.t).select(self.t.a / 1.0)

        self.assertEqual('SELECT a/1.0 FROM abc', str(q1))
        self.assertEqual('SELECT a/1.0 FROM abc', str(q2))

    def test__division__right(self):
        q1 = Query.from_('abc').select(1 / F('a'))
        q2 = Query.from_(self.t).select(1 / self.t.a)

        self.assertEqual('SELECT 1/a FROM abc', str(q1))
        self.assertEqual('SELECT 1/a FROM abc', str(q2))

    def test__complex_op(self):
        q1 = Query.from_('abc').select(2 + 1 / F('a') - 5)
        q2 = Query.from_(self.t).select(2 + 1 / self.t.a - 5)

        self.assertEqual('SELECT 2+1/a-5 FROM abc', str(q1))
        self.assertEqual('SELECT 2+1/a-5 FROM abc', str(q2))

    def test__arithmetic_equality(self):
        q1 = Query.from_('abc').select(F('a') / 2 == 2)
        q2 = Query.from_(self.t).select(self.t.a / 2 == 2)

        self.assertEqual('SELECT a/2=2 FROM abc', str(q1))
        self.assertEqual('SELECT a/2=2 FROM abc', str(q2))

    def test__arithmetic_with_function(self):
        q1 = Query.from_('abc').select(fn.Sum('foo') + 1)
        q2 = Query.from_(self.t).select(fn.Sum(self.t.foo) + 1)

        self.assertEqual('SELECT SUM(foo)+1 FROM abc', str(q1))
        self.assertEqual('SELECT SUM(foo)+1 FROM abc', str(q2))

    def test__exponent__number(self):
        q1 = Query.from_('abc').select(F('a') ** 2)
        q2 = Query.from_(self.t).select(self.t.a ** 2)

        self.assertEqual('SELECT POW(a,2) FROM abc', str(q1))
        self.assertEqual('SELECT POW(a,2) FROM abc', str(q2))

    def test__exponent__decimal(self):
        q1 = Query.from_('abc').select(F('a') ** 0.5)
        q2 = Query.from_(self.t).select(self.t.a ** 0.5)

        self.assertEqual('SELECT POW(a,0.5) FROM abc', str(q1))
        self.assertEqual('SELECT POW(a,0.5) FROM abc', str(q2))

    def test__modulus__number(self):
        q1 = Query.from_('abc').select(F('a') % 2)
        q2 = Query.from_(self.t).select(self.t.a % 2)

        self.assertEqual('SELECT MOD(a,2) FROM abc', str(q1))
        self.assertEqual('SELECT MOD(a,2) FROM abc', str(q2))


class AggregationTests(unittest.TestCase):
    def test__count(self):
        q = Query.from_('abc').select(fn.Count('foo'))

        self.assertEqual('SELECT COUNT(foo) FROM abc', str(q))

    def test__count__star(self):
        q = Query.from_('abc').select(fn.Count('*'))

        self.assertEqual('SELECT COUNT(*) FROM abc', str(q))

    def test__sum(self):
        q = Query.from_('abc').select(fn.Sum('foo'))

        self.assertEqual('SELECT SUM(foo) FROM abc', str(q))

    def test__avg(self):
        q = Query.from_('abc').select(fn.Avg('foo'))

        self.assertEqual('SELECT AVG(foo) FROM abc', str(q))

    def test__min(self):
        q = Query.from_('abc').select(fn.Min('foo'))

        self.assertEqual('SELECT MIN(foo) FROM abc', str(q))

    def test__max(self):
        q = Query.from_('abc').select(fn.Max('foo'))

        self.assertEqual('SELECT MAX(foo) FROM abc', str(q))

    def test__std(self):
        q = Query.from_('abc').select(fn.Std('foo'))

        self.assertEqual('SELECT STD(foo) FROM abc', str(q))

    def test__stddev(self):
        q = Query.from_('abc').select(fn.StdDev('foo'))

        self.assertEqual('SELECT STDDEV(foo) FROM abc', str(q))

    def test__coalesce(self):
        q = Query.from_('abc').select(fn.Coalesce('foo', 0))

        self.assertEqual('SELECT COALESCE(foo,0) FROM abc', str(q))


class ConditionTests(unittest.TestCase):
    def test__case__raw(self):
        q = Query.from_('abc').select(Case().when(F('foo') == 1, 'a').else_('b'))

        self.assertEqual("SELECT CASE WHEN foo=1 THEN 'a' ELSE 'b' END FROM abc", str(q))

    def test__case__field(self):
        q = Query.from_('abc').select(Case().when(F('foo') == 1, F('bar')).else_(F('buz')))

        self.assertEqual('SELECT CASE WHEN foo=1 THEN bar ELSE buz END FROM abc', str(q))

    def test__case__multi(self):
        q = Query.from_('abc').select(
            Case().when(
                F('foo') > 0, F('fiz')
            ).when(
                F('bar') <= 0, F('buz')
            ).else_(1)
        )

        self.assertEqual('SELECT CASE WHEN foo>0 THEN fiz WHEN bar<=0 THEN buz ELSE 1 END FROM abc', str(q))

    def test__case__no_cases(self):
        with self.assertRaises(CaseException):
            q = Query.from_('abc').select(Case())

            str(q)

    def test__case__no_else(self):
        with self.assertRaises(CaseException):
            q = Query.from_('abc').select(Case().when(F('foo') > 0, F('fiz')))

            str(q)


class StringTests(unittest.TestCase):
    t = Table('abc')

    def test__ascii__str(self):
        q = Query.select(fn.Ascii('2'))

        self.assertEqual("SELECT ASCII('2')", str(q))

    def test__ascii__int(self):
        q = Query.select(fn.Ascii(2))

        self.assertEqual("SELECT ASCII(2)", str(q))

    def test__ascii__field(self):
        q = Query.from_(self.t).select(fn.Ascii(self.t.foo))

        self.assertEqual("SELECT ASCII(foo) FROM abc", str(q))

    def test__bin__str(self):
        q = Query.select(fn.Bin('2'))

        self.assertEqual("SELECT BIN('2')", str(q))

    def test__bin__int(self):
        q = Query.select(fn.Bin(2))

        self.assertEqual("SELECT BIN(2)", str(q))

    def test__bin__field(self):
        q = Query.from_(self.t).select(fn.Bin(self.t.foo))

        self.assertEqual("SELECT BIN(foo) FROM abc", str(q))

    def test__concat__str(self):
        q = Query.select(fn.Concat('p', 'y', 'q', 'b'))

        self.assertEqual("SELECT CONCAT('p','y','q','b')", str(q))

    def test__concat__field(self):
        q = Query.from_(self.t).select(fn.Concat(self.t.foo, self.t.bar))

        self.assertEqual("SELECT CONCAT(foo,bar) FROM abc", str(q))

    def test__insert__str(self):
        q = Query.select(fn.Insert('Quadratic', 3, 4, 'What'))

        self.assertEqual("SELECT INSERT('Quadratic',3,4,'What')", str(q))

    def test__insert__field(self):
        q = Query.from_(self.t).select(fn.Insert(self.t.foo, 3, 4, self.t.bar))

        self.assertEqual("SELECT INSERT(foo,3,4,bar) FROM abc", str(q))

    def test__lower__str(self):
        q = Query.select(fn.Lower('ABC'))

        self.assertEqual("SELECT LOWER('ABC')", str(q))

    def test__lower__field(self):
        q = Query.from_(self.t).select(fn.Lower(self.t.foo))

        self.assertEqual("SELECT LOWER(foo) FROM abc", str(q))

    def test__length__str(self):
        q = Query.select(fn.Length('ABC'))

        self.assertEqual("SELECT LENGTH('ABC')", str(q))

    def test__length__field(self):
        q = Query.from_(self.t).select(fn.Length(self.t.foo))

        self.assertEqual("SELECT LENGTH(foo) FROM abc", str(q))


class CastTests(unittest.TestCase):
    t = Table('abc')

    def test__cast__as(self):
        q = Query.from_(self.t).select(fn.Cast(self.t.foo, 'UNSIGNED'))

        self.assertEqual("SELECT CAST(foo AS UNSIGNED) FROM abc", str(q))

    def test__cast__signed(self):
        q1 = Query.from_(self.t).select(fn.Signed(self.t.foo))
        q2 = Query.from_(self.t).select(fn.Cast(self.t.foo, 'SIGNED'))

        self.assertEqual("SELECT CAST(foo AS SIGNED) FROM abc", str(q1))
        self.assertEqual("SELECT CAST(foo AS SIGNED) FROM abc", str(q2))

    def test__cast__unsigned(self):
        q1 = Query.from_(self.t).select(fn.Unsigned(self.t.foo))
        q2 = Query.from_(self.t).select(fn.Cast(self.t.foo, 'UNSIGNED'))

        self.assertEqual("SELECT CAST(foo AS UNSIGNED) FROM abc", str(q1))
        self.assertEqual("SELECT CAST(foo AS UNSIGNED) FROM abc", str(q2))

    def test__convert__utf8(self):
        q = Query.from_(self.t).select(fn.Convert(self.t.foo, 'utf8'))

        self.assertEqual("SELECT CONVERT(foo USING utf8) FROM abc", str(q))

    def test__cast__date(self):
        q1 = Query.from_(self.t).select(fn.Date(self.t.foo))
        q2 = Query.from_(self.t).select(fn.Cast(self.t.foo, 'DATE'))

        self.assertEqual("SELECT DATE(foo) FROM abc", str(q1))
        self.assertEqual("SELECT CAST(foo AS DATE) FROM abc", str(q2))

    def test__cast__timestamp(self):
        q1 = Query.from_(self.t).select(fn.Timestamp(self.t.foo))
        q2 = Query.from_(self.t).select(fn.Cast(self.t.foo, 'TIMESTAMP'))

        self.assertEqual("SELECT TIMESTAMP(foo) FROM abc", str(q1))
        self.assertEqual("SELECT CAST(foo AS TIMESTAMP) FROM abc", str(q2))


class DateFunctionsTests(unittest.TestCase):
    dt = F('dt')
    t = Table('abc')

    def test_add_microsecond(self):
        c = self.dt + Interval(microseconds=1)

        self.assertEqual("dt+INTERVAL 1 MICROSECOND", str(c))

    def test_add_second(self):
        c = self.dt + Interval(seconds=1)

        self.assertEqual("dt+INTERVAL 1 SECOND", str(c))

    def test_add_minute(self):
        c = self.dt + Interval(minutes=1)

        self.assertEqual("dt+INTERVAL 1 MINUTE", str(c))

    def test_add_day(self):
        c = self.dt + Interval(days=1)

        self.assertEqual("dt+INTERVAL 1 DAY", str(c))

    def test_add_week(self):
        c = self.dt + Interval(weeks=1)

        self.assertEqual("dt+INTERVAL 1 WEEK", str(c))

    def test_add_month(self):
        c = self.dt + Interval(months=1)

        self.assertEqual("dt+INTERVAL 1 MONTH", str(c))

    def test_add_quarter(self):
        c = self.dt + Interval(quarters=1)

        self.assertEqual("dt+INTERVAL 1 QUARTER", str(c))

    def test_add_year(self):
        c = self.dt + Interval(years=1)

        self.assertEqual("dt+INTERVAL 1 YEAR", str(c))

    def test_add_second_microsecond(self):
        c = self.dt + Interval(seconds=1, microseconds=1)

        self.assertEqual("dt+INTERVAL 1.1 SECOND_MICROSECOND", str(c))

    def test_add_minute_microsecond(self):
        c = self.dt + Interval(minutes=1, microseconds=1)

        self.assertEqual("dt+INTERVAL 1:0.1 MINUTE_MICROSECOND", str(c))

    def test_add_minute_second(self):
        c = self.dt + Interval(minutes=1, seconds=1)

        self.assertEqual("dt+INTERVAL 1:1 MINUTE_SECOND", str(c))

    def test_add_hour_microsecond(self):
        c = self.dt + Interval(hours=1, microseconds=1)

        self.assertEqual("dt+INTERVAL 1:0:0.1 HOUR_MICROSECOND", str(c))

    def test_add_hour_second(self):
        c = self.dt + Interval(hours=1, seconds=1)

        self.assertEqual("dt+INTERVAL 1:0:1 HOUR_SECOND", str(c))

    def test_add_hour_minute(self):
        c = self.dt + Interval(hours=1, minutes=1)

        self.assertEqual("dt+INTERVAL 1:1 HOUR_MINUTE", str(c))

    def test_add_day_microsecond(self):
        c = self.dt + Interval(days=1, microseconds=1)

        self.assertEqual("dt+INTERVAL 1 0:0:0.1 DAY_MICROSECOND", str(c))

    def test_add_day_second(self):
        c = self.dt + Interval(days=1, seconds=1)

        self.assertEqual("dt+INTERVAL 1 0:0:1 DAY_SECOND", str(c))

    def test_add_day_minute(self):
        c = self.dt + Interval(days=1, minutes=1)

        self.assertEqual("dt+INTERVAL 1 0:1 DAY_MINUTE", str(c))

    def test_add_day_hour(self):
        c = self.dt + Interval(days=1, hours=1)

        self.assertEqual("dt+INTERVAL 1 1 DAY_HOUR", str(c))

    def test_add_year_month(self):
        c = self.dt + Interval(years=1, months=1)

        self.assertEqual("dt+INTERVAL 1-1 YEAR_MONTH", str(c))

    def test_add_value_right(self):
        c = Interval(microseconds=1) - self.dt

        self.assertEqual("INTERVAL 1 MICROSECOND-dt", str(c))

    def test_add_value_complex_expressions(self):
        c = self.dt + Interval(quarters=1) + Interval(weeks=1)

        self.assertEqual("dt+INTERVAL 1 QUARTER+INTERVAL 1 WEEK", str(c))

    def _test_extract_datepart(self, date_part):
        q = Query.from_(self.t).select(fn.Extract(date_part, self.t.foo))

        self.assertEqual("SELECT EXTRACT(%s FROM foo) FROM abc" % date_part.value, str(q))

    def test_extract_microsecond(self):
        self._test_extract_datepart(DatePart.microsecond)

    def test_extract_second(self):
        self._test_extract_datepart(DatePart.second)

    def test_extract_minute(self):
        self._test_extract_datepart(DatePart.minute)

    def test_extract_hour(self):
        self._test_extract_datepart(DatePart.hour)

    def test_extract_day(self):
        self._test_extract_datepart(DatePart.day)

    def test_extract_week(self):
        self._test_extract_datepart(DatePart.week)

    def test_extract_month(self):
        self._test_extract_datepart(DatePart.month)

    def test_extract_quarter(self):
        self._test_extract_datepart(DatePart.quarter)

    def test_extract_year(self):
        self._test_extract_datepart(DatePart.year)
