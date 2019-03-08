import unittest

from pypika import (
    Case,
    CaseException,
    DatePart,
    Field as F,
    Query,
    Query as Q,
    Schema,
    Table as T,
    VerticaQuery,
    functions as fn,
)
from pypika.enums import SqlTypes

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class SchemaTests(unittest.TestCase):
    def test_schema_no_schema_in_sql_when_none_set(self):
        func = fn.Function('my_proc', 1, 2, 3)

        self.assertEqual('my_proc(1,2,3)', func.get_sql(quote_char='"'))

    def test_schema_included_in_function_sql(self):
        a = Schema('a')
        func = fn.Function('my_proc', 1, 2, 3, schema=a)

        self.assertEqual('"a".my_proc(1,2,3)', func.get_sql(quote_char='"'))


class ArithmeticTests(unittest.TestCase):
    t = T('abc')

    def test__addition__fields(self):
        q1 = Q.from_('abc').select(F('a') + F('b'))
        q2 = Q.from_(self.t).select(self.t.a + self.t.b)

        self.assertEqual('SELECT \"a\"+\"b\" FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"+\"b\" FROM \"abc\"', str(q2))

    def test__addition__number(self):
        q1 = Q.from_('abc').select(F('a') + 1)
        q2 = Q.from_(self.t).select(self.t.a + 1)

        self.assertEqual('SELECT \"a\"+1 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"+1 FROM \"abc\"', str(q2))

    def test__addition__decimal(self):
        q1 = Q.from_('abc').select(F('a') + 1.0)
        q2 = Q.from_(self.t).select(self.t.a + 1.0)

        self.assertEqual('SELECT \"a\"+1.0 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"+1.0 FROM \"abc\"', str(q2))

    def test__addition__right(self):
        q1 = Q.from_('abc').select(1 + F('a'))
        q2 = Q.from_(self.t).select(1 + self.t.a)

        self.assertEqual('SELECT 1+\"a\" FROM \"abc\"', str(q1))
        self.assertEqual('SELECT 1+\"a\" FROM \"abc\"', str(q2))

    def test__subtraction__fields(self):
        q1 = Q.from_('abc').select(F('a') - F('b'))
        q2 = Q.from_(self.t).select(self.t.a - self.t.b)

        self.assertEqual('SELECT \"a\"-\"b\" FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"-\"b\" FROM \"abc\"', str(q2))

    def test__subtraction__number(self):
        q1 = Q.from_('abc').select(F('a') - 1)
        q2 = Q.from_(self.t).select(self.t.a - 1)

        self.assertEqual('SELECT \"a\"-1 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"-1 FROM \"abc\"', str(q2))

    def test__subtraction__decimal(self):
        q1 = Q.from_('abc').select(F('a') - 1.0)
        q2 = Q.from_(self.t).select(self.t.a - 1.0)

        self.assertEqual('SELECT \"a\"-1.0 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"-1.0 FROM \"abc\"', str(q2))

    def test__subtraction__right(self):
        q1 = Q.from_('abc').select(1 - F('a'))
        q2 = Q.from_(self.t).select(1 - self.t.a)

        self.assertEqual('SELECT 1-\"a\" FROM \"abc\"', str(q1))
        self.assertEqual('SELECT 1-\"a\" FROM \"abc\"', str(q2))

    def test__multiplication__fields(self):
        q1 = Q.from_('abc').select(F('a') * F('b'))
        q2 = Q.from_(self.t).select(self.t.a * self.t.b)

        self.assertEqual('SELECT \"a\"*\"b\" FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"*\"b\" FROM \"abc\"', str(q2))

    def test__multiplication__number(self):
        q1 = Q.from_('abc').select(F('a') * 1)
        q2 = Q.from_(self.t).select(self.t.a * 1)

        self.assertEqual('SELECT \"a\"*1 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"*1 FROM \"abc\"', str(q2))

    def test__multiplication__decimal(self):
        q1 = Q.from_('abc').select(F('a') * 1.0)
        q2 = Q.from_(self.t).select(self.t.a * 1.0)

        self.assertEqual('SELECT \"a\"*1.0 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"*1.0 FROM \"abc\"', str(q2))

    def test__multiplication__right(self):
        q1 = Q.from_('abc').select(1 * F('a'))
        q2 = Q.from_(self.t).select(1 * self.t.a)

        self.assertEqual('SELECT 1*\"a\" FROM \"abc\"', str(q1))
        self.assertEqual('SELECT 1*\"a\" FROM \"abc\"', str(q2))

    def test__division__fields(self):
        q1 = Q.from_('abc').select(F('a') / F('b'))
        q2 = Q.from_(self.t).select(self.t.a / self.t.b)

        self.assertEqual('SELECT \"a\"/\"b\" FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"/\"b\" FROM \"abc\"', str(q2))

    def test__division__number(self):
        q1 = Q.from_('abc').select(F('a') / 1)
        q2 = Q.from_(self.t).select(self.t.a / 1)

        self.assertEqual('SELECT \"a\"/1 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"/1 FROM \"abc\"', str(q2))

    def test__division__decimal(self):
        q1 = Q.from_('abc').select(F('a') / 1.0)
        q2 = Q.from_(self.t).select(self.t.a / 1.0)

        self.assertEqual('SELECT \"a\"/1.0 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"/1.0 FROM \"abc\"', str(q2))

    def test__division__right(self):
        q1 = Q.from_('abc').select(1 / F('a'))
        q2 = Q.from_(self.t).select(1 / self.t.a)

        self.assertEqual('SELECT 1/\"a\" FROM \"abc\"', str(q1))
        self.assertEqual('SELECT 1/\"a\" FROM \"abc\"', str(q2))

    def test__complex_op(self):
        q1 = Q.from_('abc').select(2 + 1 / F('a') - 5)
        q2 = Q.from_(self.t).select(2 + 1 / self.t.a - 5)

        self.assertEqual('SELECT 2+1/\"a\"-5 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT 2+1/\"a\"-5 FROM \"abc\"', str(q2))

    def test__complex_op_add_parentheses(self):
        q1 = Q.from_('abc').select((F('a') + 1) + (F('b') - 5))
        q2 = Q.from_(self.t).select((self.t.a + 1) + (self.t.b - 5))

        self.assertEqual('SELECT \"a\"+1+\"b\"-5 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"+1+\"b\"-5 FROM \"abc\"', str(q2))

    def test__complex_op_sub_parentheses(self):
        q1 = Q.from_('abc').select((F('a') + 1) - (F('b') - 5))
        q2 = Q.from_(self.t).select((self.t.a + 1) - (self.t.b - 5))

        self.assertEqual('SELECT \"a\"+1-\"b\"-5 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"+1-\"b\"-5 FROM \"abc\"', str(q2))

    def test__complex_op_mul_parentheses(self):
        q1 = Q.from_('abc').select((F('a') + 1) * (F('b') - 5))
        q2 = Q.from_(self.t).select((self.t.a + 1) * (self.t.b - 5))

        self.assertEqual('SELECT (\"a\"+1)*(\"b\"-5) FROM \"abc\"', str(q1))
        self.assertEqual('SELECT (\"a\"+1)*(\"b\"-5) FROM \"abc\"', str(q2))

    def test__complex_op_mul_no_parentheses(self):
        q = Q.from_('abc').select(F('a') + 1 * F('b') - 5)

        self.assertEqual('SELECT \"a\"+1*\"b\"-5 FROM \"abc\"', str(q))

    def test__complex_op_div_parentheses(self):
        q1 = Q.from_('abc').select((F('a') + 1) / (F('b') - 5))
        q2 = Q.from_(self.t).select((self.t.a + 1) / (self.t.b - 5))

        self.assertEqual('SELECT (\"a\"+1)/(\"b\"-5) FROM \"abc\"', str(q1))
        self.assertEqual('SELECT (\"a\"+1)/(\"b\"-5) FROM \"abc\"', str(q2))

    def test__complex_op_div_no_parentheses(self):
        q = Q.from_('abc').select(F('a') + 1 / F('b') - 5)

        self.assertEqual('SELECT \"a\"+1/\"b\"-5 FROM \"abc\"', str(q))

    def test__arithmetic_equality(self):
        q1 = Q.from_('abc').select(F('a') / 2 == 2)
        q2 = Q.from_(self.t).select(self.t.a / 2 == 2)

        self.assertEqual('SELECT "a"/2=2 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT \"a\"/2=2 FROM \"abc\"', str(q2))

    def test__arithmetic_with_function(self):
        q1 = Q.from_('abc').select(fn.Sum(F('foo')) + 1)
        q2 = Q.from_(self.t).select(fn.Sum(self.t.foo) + 1)

        self.assertEqual('SELECT SUM(\"foo\")+1 FROM \"abc\"', str(q1))
        self.assertEqual('SELECT SUM(\"foo\")+1 FROM \"abc\"', str(q2))

    def test__exponent__number(self):
        q1 = Q.from_('abc').select(F('a') ** 2)
        q2 = Q.from_(self.t).select(self.t.a ** 2)

        self.assertEqual('SELECT POW(\"a\",2) FROM \"abc\"', str(q1))
        self.assertEqual('SELECT POW(\"a\",2) FROM \"abc\"', str(q2))

    def test__exponent__decimal(self):
        q1 = Q.from_('abc').select(F('a') ** 0.5)
        q2 = Q.from_(self.t).select(self.t.a ** 0.5)

        self.assertEqual('SELECT POW(\"a\",0.5) FROM \"abc\"', str(q1))
        self.assertEqual('SELECT POW(\"a\",0.5) FROM \"abc\"', str(q2))

    def test__modulus__number(self):
        q1 = Q.from_('abc').select(F('a') % 2)
        q2 = Q.from_(self.t).select(self.t.a % 2)

        self.assertEqual('SELECT MOD(\"a\",2) FROM \"abc\"', str(q1))
        self.assertEqual('SELECT MOD(\"a\",2) FROM \"abc\"', str(q2))

    def test__floor(self):
        q1 = Q.from_('abc').select(fn.Floor(F('foo')))
        q2 = Q.from_(self.t).select(fn.Floor(self.t.foo))

        self.assertEqual('SELECT FLOOR(\"foo\") FROM \"abc\"', str(q1))
        self.assertEqual('SELECT FLOOR(\"foo\") FROM \"abc\"', str(q2))


class AggregationTests(unittest.TestCase):
    def test__count(self):
        q = Q.from_('abc').select(fn.Count(F('foo')))

        self.assertEqual('SELECT COUNT(\"foo\") FROM \"abc\"', str(q))

    def test__count__star(self):
        q = Q.from_('abc').select(fn.Count('*'))

        self.assertEqual('SELECT COUNT(*) FROM \"abc\"', str(q))

    def test__sum(self):
        q = Q.from_('abc').select(fn.Sum(F('foo')))

        self.assertEqual('SELECT SUM(\"foo\") FROM \"abc\"', str(q))

    def test__approx_count_distinct(self):
        q = Q.from_('abc').select(fn.ApproxCountDistinct(F('foo')))

        self.assertEqual('SELECT APPROX_COUNT_DISTINCT(\"foo\") FROM \"abc\"', str(q))

    def test__avg(self):
        q = Q.from_('abc').select(fn.Avg(F('foo')))

        self.assertEqual('SELECT AVG(\"foo\") FROM \"abc\"', str(q))

    def test__min(self):
        q = Q.from_('abc').select(fn.Min(F('foo')))

        self.assertEqual('SELECT MIN(\"foo\") FROM \"abc\"', str(q))

    def test__max(self):
        q = Q.from_('abc').select(fn.Max(F('foo')))

        self.assertEqual('SELECT MAX(\"foo\") FROM \"abc\"', str(q))

    def test__std(self):
        q = Q.from_('abc').select(fn.Std(F('foo')))

        self.assertEqual('SELECT STD(\"foo\") FROM \"abc\"', str(q))

    def test__stddev(self):
        q = Q.from_('abc').select(fn.StdDev(F('foo')))

        self.assertEqual('SELECT STDDEV(\"foo\") FROM \"abc\"', str(q))


class ConditionTests(unittest.TestCase):
    def test__case__raw(self):
        q = Q.from_('abc').select(Case().when(F('foo') == 1, 'a'))

        self.assertEqual("SELECT CASE WHEN \"foo\"=1 THEN 'a' END FROM \"abc\"", str(q))

    def test__case__else(self):
        q = Q.from_('abc').select(Case().when(F('foo') == 1, 'a').else_('b'))

        self.assertEqual("SELECT CASE WHEN \"foo\"=1 THEN 'a' ELSE 'b' END FROM \"abc\"", str(q))

    def test__case__field(self):
        q = Q.from_('abc').select(Case().when(F('foo') == 1, F('bar')).else_(F('buz')))

        self.assertEqual(
            'SELECT CASE WHEN \"foo\"=1 THEN \"bar\" ELSE \"buz\" END FROM \"abc\"', str(q))

    def test__case__multi(self):
        q = Q.from_('abc').select(
            Case().when(
                F('foo') > 0, F('fiz')
            ).when(
                F('bar') <= 0, F('buz')
            ).else_(1)
        )

        self.assertEqual('SELECT CASE WHEN \"foo\">0 THEN \"fiz\" WHEN \"bar\"<=0 THEN \"buz\" ELSE 1 END FROM \"abc\"',
                         str(q))

    def test__case__no_cases(self):
        with self.assertRaises(CaseException):
            q = Q.from_('abc').select(Case())

            str(q)


class StringTests(unittest.TestCase):
    t = T('abc')

    def test__ascii__str(self):
        q = Q.select(fn.Ascii('2'))

        self.assertEqual("SELECT ASCII('2')", str(q))

    def test__ascii__int(self):
        q = Q.select(fn.Ascii(2))

        self.assertEqual("SELECT ASCII(2)", str(q))

    def test__ascii__field(self):
        q = Q.from_(self.t).select(fn.Ascii(self.t.foo))

        self.assertEqual("SELECT ASCII(\"foo\") FROM \"abc\"", str(q))

    def test__bin__str(self):
        q = Q.select(fn.Bin('2'))

        self.assertEqual("SELECT BIN('2')", str(q))

    def test__bin__int(self):
        q = Q.select(fn.Bin(2))

        self.assertEqual("SELECT BIN(2)", str(q))

    def test__bin__field(self):
        q = Q.from_(self.t).select(fn.Bin(self.t.foo))

        self.assertEqual("SELECT BIN(\"foo\") FROM \"abc\"", str(q))

    def test__concat__str(self):
        q = Q.select(fn.Concat('p', 'y', 'q', 'b'))

        self.assertEqual("SELECT CONCAT('p','y','q','b')", str(q))

    def test__concat__field(self):
        q = Q.from_(self.t).select(fn.Concat(self.t.foo, self.t.bar))

        self.assertEqual("SELECT CONCAT(\"foo\",\"bar\") FROM \"abc\"", str(q))

    def test__insert__str(self):
        q = Q.select(fn.Insert('Quadratic', 3, 4, 'What'))

        self.assertEqual("SELECT INSERT('Quadratic',3,4,'What')", str(q))

    def test__insert__field(self):
        q = Q.from_(self.t).select(fn.Insert(self.t.foo, 3, 4, self.t.bar))

        self.assertEqual("SELECT INSERT(\"foo\",3,4,\"bar\") FROM \"abc\"", str(q))

    def test__lower__str(self):
        q = Q.select(fn.Lower('ABC'))

        self.assertEqual("SELECT LOWER('ABC')", str(q))

    def test__lower__field(self):
        q = Q.from_(self.t).select(fn.Lower(self.t.foo))

        self.assertEqual("SELECT LOWER(\"foo\") FROM \"abc\"", str(q))

    def test__length__str(self):
        q = Q.select(fn.Length('ABC'))

        self.assertEqual("SELECT LENGTH('ABC')", str(q))

    def test__length__field(self):
        q = Q.from_(self.t).select(fn.Length(self.t.foo))

        self.assertEqual("SELECT LENGTH(\"foo\") FROM \"abc\"", str(q))

    def test__substring(self):
        q = Q.from_(self.t).select(fn.Substring(self.t.foo, 2, 6))

        self.assertEqual("SELECT SUBSTRING(\"foo\",2,6) FROM \"abc\"", str(q))


class SplitPartFunctionTests(unittest.TestCase):
    t = T('abc')

    def test__split_part(self):
        q = VerticaQuery.from_(self.t).select(fn.SplitPart(self.t.foo, '|', 3))

        self.assertEqual("SELECT SPLIT_PART(\"foo\",\'|\',3) FROM \"abc\"", str(q))


class RegexpLikeFunctionTests(unittest.TestCase):
    t = T('abc')

    def test__regexp_like(self):
        q = VerticaQuery.from_(self.t).select(fn.RegexpLike(self.t.foo, '^a', 'x'))

        self.assertEqual("SELECT REGEXP_LIKE(\"foo\",\'^a\',\'x\') FROM \"abc\"", str(q))


class CastTests(unittest.TestCase):
    t = T('abc')

    def test__cast__as(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.UNSIGNED))

        self.assertEqual("SELECT CAST(\"foo\" AS UNSIGNED) FROM \"abc\"", str(q))

    def test__cast__signed(self):
        q1 = Q.from_(self.t).select(fn.Signed(self.t.foo))
        q2 = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.SIGNED))

        self.assertEqual("SELECT CAST(\"foo\" AS SIGNED) FROM \"abc\"", str(q1))
        self.assertEqual("SELECT CAST(\"foo\" AS SIGNED) FROM \"abc\"", str(q2))

    def test__cast__unsigned(self):
        q1 = Q.from_(self.t).select(fn.Unsigned(self.t.foo))
        q2 = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.UNSIGNED))

        self.assertEqual("SELECT CAST(\"foo\" AS UNSIGNED) FROM \"abc\"", str(q1))
        self.assertEqual("SELECT CAST(\"foo\" AS UNSIGNED) FROM \"abc\"", str(q2))

    def test__cast__date(self):
        q1 = Q.from_(self.t).select(fn.Date(self.t.foo))
        q2 = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.DATE))

        self.assertEqual("SELECT DATE(\"foo\") FROM \"abc\"", str(q1))
        self.assertEqual("SELECT CAST(\"foo\" AS DATE) FROM \"abc\"", str(q2))

    def test__cast__timestamp(self):
        q1 = Q.from_(self.t).select(fn.Timestamp(self.t.foo))
        q2 = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.TIMESTAMP))

        self.assertEqual("SELECT TIMESTAMP(\"foo\") FROM \"abc\"", str(q1))
        self.assertEqual("SELECT CAST(\"foo\" AS TIMESTAMP) FROM \"abc\"", str(q2))

    def test__cast__char(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.CHAR))

        self.assertEqual("SELECT CAST(\"foo\" AS CHAR) FROM \"abc\"", str(q))

    def test__cast__char_with_arg(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.VARCHAR(24)))

        self.assertEqual("SELECT CAST(\"foo\" AS VARCHAR(24)) FROM \"abc\"", str(q))

    def test__cast__varchar(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.VARCHAR))

        self.assertEqual("SELECT CAST(\"foo\" AS VARCHAR) FROM \"abc\"", str(q))

    def test__cast__varchar_with_arg(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.VARCHAR(24)))

        self.assertEqual("SELECT CAST(\"foo\" AS VARCHAR(24)) FROM \"abc\"", str(q))

    def test__cast__long_varchar(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.LONG_VARCHAR))

        self.assertEqual("SELECT CAST(\"foo\" AS LONG VARCHAR) FROM \"abc\"", str(q))

    def test__cast__long_varchar_with_arg(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.LONG_VARCHAR(24)))

        self.assertEqual("SELECT CAST(\"foo\" AS LONG VARCHAR(24)) FROM \"abc\"", str(q))

    def test__cast__binary(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.BINARY))

        self.assertEqual("SELECT CAST(\"foo\" AS BINARY) FROM \"abc\"", str(q))

    def test__cast__binary_with_arg(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.BINARY(24)))

        self.assertEqual("SELECT CAST(\"foo\" AS BINARY(24)) FROM \"abc\"", str(q))

    def test__cast__varbinary(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.VARBINARY))

        self.assertEqual("SELECT CAST(\"foo\" AS VARBINARY) FROM \"abc\"", str(q))

    def test__cast__varbinary_with_arg(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.VARBINARY(24)))

        self.assertEqual("SELECT CAST(\"foo\" AS VARBINARY(24)) FROM \"abc\"", str(q))

    def test__cast__long_varbinary(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.LONG_VARBINARY))

        self.assertEqual("SELECT CAST(\"foo\" AS LONG VARBINARY) FROM \"abc\"", str(q))

    def test__cast__long_varbinary_with_arg(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.LONG_VARBINARY(24)))

        self.assertEqual("SELECT CAST(\"foo\" AS LONG VARBINARY(24)) FROM \"abc\"", str(q))

    def test__cast__boolean(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.BOOLEAN))

        self.assertEqual("SELECT CAST(\"foo\" AS BOOLEAN) FROM \"abc\"", str(q))

    def test__cast__integer(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.INTEGER))

        self.assertEqual("SELECT CAST(\"foo\" AS INTEGER) FROM \"abc\"", str(q))

    def test__cast__float(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.FLOAT))

        self.assertEqual("SELECT CAST(\"foo\" AS FLOAT) FROM \"abc\"", str(q))

    def test__cast__numeric(self):
        q = Q.from_(self.t).select(fn.Cast(self.t.foo, SqlTypes.NUMERIC))

        self.assertEqual("SELECT CAST(\"foo\" AS NUMERIC) FROM \"abc\"", str(q))

    def test__tochar__(self):
        q = Q.from_(self.t).select(fn.ToChar(self.t.foo, "SomeFormat"))

        self.assertEqual("SELECT TO_CHAR(\"foo\",'SomeFormat') FROM \"abc\"", str(q))


class DateFunctionsTests(unittest.TestCase):
    dt = F('dt')
    t = T('abc')

    def _test_extract_datepart(self, date_part):
        q = Q.from_(self.t).select(fn.Extract(date_part, self.t.foo))

        self.assertEqual("SELECT EXTRACT(%s FROM \"foo\") FROM \"abc\"" % date_part.value, str(q))

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

    def test_timestampadd(self):
        a = fn.TimestampAdd('year', 1, '2017-10-01')
        self.assertEqual(str(a), "TIMESTAMPADD('year',1,'2017-10-01')")

    def test_date_add(self):
        a = fn.DateAdd('year', 1, '2017-10-01')
        self.assertEqual(str(a), "DATE_ADD('year',1,'2017-10-01')")

    def test_now(self):
        query = Query.select(fn.Now())

        self.assertEqual("SELECT NOW()", str(query))

    def test_utc_timestamp(self):
        query = Query.select(fn.UtcTimestamp())

        self.assertEqual("SELECT UTC_TIMESTAMP()", str(query))

    def test_current_date(self):
        query = Query.select(fn.CurDate())

        self.assertEqual("SELECT CURRENT_DATE()", str(query))

    def test_current_time(self):
        query = Query.select(fn.CurTime())

        self.assertEqual("SELECT CURRENT_TIME()", str(query))


class NullFunctionsTests(unittest.TestCase):
    def test_isnull(self):
        q = Q.from_('abc').select(fn.IsNull(F('foo')))

        self.assertEqual('SELECT ISNULL(\"foo\") FROM \"abc\"', str(q))

    def test_coalesce(self):
        q = Q.from_('abc').select(fn.Coalesce(F('foo'), 0))

        self.assertEqual('SELECT COALESCE(\"foo\",0) FROM \"abc\"', str(q))

    def test_nullif(self):
        q = Q.from_('abc').select(fn.NullIf(F('foo') == 0))

        self.assertEqual('SELECT NULLIF(\"foo\"=0) FROM \"abc\"', str(q))

    def test_nvl(self):
        q = Q.from_('abc').select(fn.NVL(F('foo'), 0))

        self.assertEqual('SELECT NVL(\"foo\",0) FROM \"abc\"', str(q))
