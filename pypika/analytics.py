# coding: utf-8
"""
Package for SQL analytic functions wrappers
"""
from pypika.terms import AnalyticFunction, IgnoreNullsAnalyticFunction

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class Rank(AnalyticFunction):
    def __init__(self, **kwargs):
        super(Rank, self).__init__('RANK', **kwargs)


class NTile(AnalyticFunction):
    def __init__(self, term, **kwargs):
        super(NTile, self).__init__('NTILE', term, **kwargs)


class FirstValue(IgnoreNullsAnalyticFunction):
    def __init__(self, *terms, **kwargs):
        super(FirstValue, self).__init__('FIRST_VALUE', *terms, **kwargs)


class LastValue(IgnoreNullsAnalyticFunction):
    def __init__(self, *terms, **kwargs):
        super(LastValue, self).__init__('LAST_VALUE', *terms, **kwargs)


class Median(AnalyticFunction):
    def __init__(self, term, **kwargs):
        super(Median, self).__init__('MEDIAN', term, **kwargs)


class Avg(AnalyticFunction):
    def __init__(self, term, **kwargs):
        super(Avg, self).__init__('AVG', term, **kwargs)


class StdDev(AnalyticFunction):
    def __init__(self, term, **kwargs):
        super(StdDev, self).__init__('STDDEV', term, **kwargs)
