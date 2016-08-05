# coding: utf-8
"""
Package for SQL functions wrappers
"""
from pypika.enums import SqlTypes
from pypika.terms import Function, Star
from pypika.utils import builder

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class Count(Function):
    def __init__(self, param, alias=None):
        is_star = isinstance(param, str) and '*' == param
        super(Count, self).__init__('COUNT', Star() if is_star else param, alias=alias)
        self._distinct = False

    def get_sql(self, **kwargs):
        s = super(Count, self).get_sql(**kwargs)
        if self._distinct:
            return s[:6] + 'DISTINCT ' + s[6:]
        return s

    @builder
    def distinct(self):
        self._distinct = True
        return self


# Arithmetic Functions
class Sum(Function):
    def __init__(self, term, alias=None):
        super(Sum, self).__init__('SUM', term, alias=alias)


class Avg(Function):
    def __init__(self, term, alias=None):
        super(Avg, self).__init__('AVG', term, alias=alias)


class Min(Function):
    def __init__(self, term, alias=None):
        super(Min, self).__init__('MIN', term, alias=alias)


class Max(Function):
    def __init__(self, term, alias=None):
        super(Max, self).__init__('MAX', term, alias=alias)


class Std(Function):
    def __init__(self, term, alias=None):
        super(Std, self).__init__('STD', term, alias=alias)


class StdDev(Function):
    def __init__(self, term, alias=None):
        super(StdDev, self).__init__('STDDEV', term, alias=alias)


class Coalesce(Function):
    def __init__(self, term, default_value, alias=None):
        super(Coalesce, self).__init__('COALESCE', term, default_value, alias=alias)


# Type Functions
class Cast(Function):
    def __init__(self, term, as_type, alias=None):
        super(Cast, self).__init__('CAST', term, as_type, alias=alias)

    def get_sql(self, **kwargs):
        # FIXME escape
        return '{name}({field} AS {type})'.format(
            name=self.name,
            field=self.params[0],
            type=self.params[1],
        )


class Convert(Function):
    def __init__(self, term, encoding, alias=None):
        super(Convert, self).__init__('CONVERT', term, encoding, alias=alias)

    def get_sql(self, **kwargs):
        # FIXME escape
        return '{name}({field} USING {type})'.format(
            name=self.name,
            field=self.params[0],
            type=self.params[1],
        )


class Signed(Cast):
    def __init__(self, term, alias=None):
        super(Signed, self).__init__(term, SqlTypes.SIGNED, alias=alias)


class Unsigned(Cast):
    def __init__(self, term, alias=None):
        super(Unsigned, self).__init__(term, SqlTypes.UNSIGNED, alias=alias)


class Date(Function):
    def __init__(self, term, alias=None):
        super(Date, self).__init__('DATE', term, alias=alias)


class Timestamp(Function):
    def __init__(self, term, alias=None):
        super(Timestamp, self).__init__('TIMESTAMP', term, alias=alias)


# String Functions
class Ascii(Function):
    def __init__(self, term, alias=None):
        super(Ascii, self).__init__('ASCII', term, alias=alias)


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


class Lower(Function):
    def __init__(self, term, alias=None):
        super(Lower, self).__init__('LOWER', term, alias=alias)


# Date Functions
class Now(Function):
    def __init__(self, alias=None):
        super(Now, self).__init__('NOW', alias=alias)


class CurDate(Function):
    def __init__(self, alias=None):
        super(CurDate, self).__init__('CURDATE', alias=alias)


class CurTime(Function):
    def __init__(self, alias=None):
        super(CurTime, self).__init__('CURTIME', alias=alias)


class Extract(Function):
    def __init__(self, date_part, field, alias=None):
        super(Extract, self).__init__('EXTRACT', date_part, field, alias=alias)

    def get_sql(self, **kwargs):
        return '{name}({part} FROM {field})'.format(
            name=self.name,
            part=self.params[0],
            field=self.params[1],
        )
