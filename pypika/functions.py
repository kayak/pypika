# coding: utf-8
"""
Package for SQL functions wrappers
"""
from pypika.enums import (
    SqlTypes,
)
from pypika.terms import (
    AggregateFunction,
    Function,
    Star,
)
from pypika.utils import builder

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class Count(AggregateFunction):
    def __init__(self, param, alias=None):
        is_star = isinstance(param, str) and '*' == param
        super(Count, self).__init__('COUNT', Star() if is_star else param, alias=alias)
        self._distinct = False

    def get_function_sql(self, **kwargs):
        s = super(Count, self).get_function_sql(**kwargs)
        if self._distinct:
            return s[:6] + 'DISTINCT ' + s[6:]
        return s

    @builder
    def distinct(self):
        self._distinct = True


# Arithmetic Functions
class Sum(AggregateFunction):
    def __init__(self, term, alias=None):
        super(Sum, self).__init__('SUM', term, alias=alias)


class Avg(AggregateFunction):
    def __init__(self, term, alias=None):
        super(Avg, self).__init__('AVG', term, alias=alias)


class Min(AggregateFunction):
    def __init__(self, term, alias=None):
        super(Min, self).__init__('MIN', term, alias=alias)


class Max(AggregateFunction):
    def __init__(self, term, alias=None):
        super(Max, self).__init__('MAX', term, alias=alias)


class Std(AggregateFunction):
    def __init__(self, term, alias=None):
        super(Std, self).__init__('STD', term, alias=alias)


class StdDev(AggregateFunction):
    def __init__(self, term, alias=None):
        super(StdDev, self).__init__('STDDEV', term, alias=alias)


class Coalesce(Function):
    def __init__(self, term, default_value, alias=None):
        super(Coalesce, self).__init__('COALESCE', term, default_value, alias=alias)


# Type Functions
class Cast(Function):
    def __init__(self, term, as_type, alias=None):
        super(Cast, self).__init__('CAST', term, alias=alias)
        self.as_type = as_type

    def get_special_params_sql(self, **kwargs):
        return 'AS {type}'.format(type=self.as_type.value)


class Convert(Function):
    def __init__(self, term, encoding, alias=None):
        super(Convert, self).__init__('CONVERT', term, alias=alias)
        self.encoding = encoding

    def get_special_params_sql(self, **kwargs):
        return 'USING {type}'.format(type=self.encoding.value)


class ToChar(Function):
    def __init__(self, term, as_type, alias=None):
        super(ToChar, self).__init__('TO_CHAR', term, as_type, alias=alias)


class Signed(Cast):
    def __init__(self, term, alias=None):
        super(Signed, self).__init__(term, SqlTypes.SIGNED, alias=alias)


class Unsigned(Cast):
    def __init__(self, term, alias=None):
        super(Unsigned, self).__init__(term, SqlTypes.UNSIGNED, alias=alias)


class Date(Function):
    def __init__(self, term, alias=None):
        super(Date, self).__init__('DATE', term, alias=alias)


class DateDiff(Function):
    def __init__(self, interval, start_date, end_date, alias=None):
        super(DateDiff, self).__init__('DATEDIFF', interval, start_date, end_date, alias=alias)


class DateAdd(Function):
    def __init__(self, date_part, interval, term, alias=None):
        super(DateAdd, self).__init__('DATE_ADD', date_part, interval, term, alias=alias)


class Timestamp(Function):
    def __init__(self, term, alias=None):
        super(Timestamp, self).__init__('TIMESTAMP', term, alias=alias)


class TimestampAdd(Function):
    def __init__(self, date_part, interval, term, alias=None):
        super(TimestampAdd, self).__init__('TIMESTAMPADD', date_part, interval, term, alias=alias)


# String Functions
class Ascii(Function):
    def __init__(self, term, alias=None):
        super(Ascii, self).__init__('ASCII', term, alias=alias)


class NullIf(Function):
    def __init__(self, term, criterion, alias=None):
        super(NullIf, self).__init__('NULLIF', term, criterion, alias=alias)


class Bin(Function):
    def __init__(self, term, alias=None):
        super(Bin, self).__init__('BIN', term, alias=alias)


class Concat(Function):
    def __init__(self, *terms, **kwargs):
        super(Concat, self).__init__('CONCAT', *terms, **kwargs)


class Insert(Function):
    def __init__(self, term, start, stop, subterm, alias=None):
        term, start, stop, subterm = [term for term in [term, start, stop, subterm]]
        super(Insert, self).__init__('INSERT', term, start, stop, subterm, alias=alias)


class Length(Function):
    def __init__(self, term, alias=None):
        super(Length, self).__init__('LENGTH', term, alias=alias)


class Upper(Function):
    def __init__(self, term, alias=None):
        super(Upper, self).__init__('UPPER', term, alias=alias)


class Lower(Function):
    def __init__(self, term, alias=None):
        super(Lower, self).__init__('LOWER', term, alias=alias)


class Substring(Function):
    def __init__(self, term, alias=None):
        super(Substring, self).__init__('SUBSTRING', term, alias=alias)


class Reverse(Function):
    def __init__(self, term, alias=None):
        super(Reverse, self).__init__('REVERSE', term, alias=alias)


class Trim(Function):
    def __init__(self, term, alias=None):
        super(Trim, self).__init__('TRIM', term, alias=alias)


class SplitPart(Function):
    def __init__(self, term, delimiter, index, alias=None):
        super(SplitPart, self).__init__('SPLIT_PART', term, delimiter, index, alias=alias)


class RegexpMatches(Function):
    def __init__(self, term, pattern, modifiers=None, alias=None):
        super(RegexpMatches, self).__init__('REGEXP_MATCHES', term, pattern, modifiers, alias=alias)


class RegexpLike(Function):
    def __init__(self, term, pattern, modifiers=None, alias=None):
        super(RegexpLike, self).__init__('REGEXP_LIKE', term, pattern, modifiers, alias=alias)


# Date Functions
class Now(Function):
    def __init__(self, alias=None):
        super(Now, self).__init__('NOW', alias=alias)


class CurDate(Function):
    def __init__(self, alias=None):
        super(CurDate, self).__init__('CURRENT_DATE', alias=alias)


class CurTime(Function):
    def __init__(self, alias=None):
        super(CurTime, self).__init__('CURRENT_TIME', alias=alias)


class Extract(Function):
    def __init__(self, date_part, field, alias=None):
        super(Extract, self).__init__('EXTRACT', date_part, alias=alias)
        self.field = field

    def get_special_params_sql(self, **kwargs):
        return 'FROM {field}'.format(
            field=self.field,
        )
