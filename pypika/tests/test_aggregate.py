# coding: utf-8
import unittest

from pypika import Case
from pypika import Field, functions as fn


class IsAggregateTests(unittest.TestCase):
    def test__field_is_not_aggregate(self):
        v = Field('foo')
        self.assertFalse(v.is_aggregate)

    def test__field_arithmetic_is_not_aggregate(self):
        v = Field('foo') + Field('bar')
        self.assertFalse(v.is_aggregate)

    def test__agg_func_is_aggregate(self):
        v = fn.Sum(Field('foo'))
        self.assertTrue(v.is_aggregate)

    def test__agg_func_arithmetic_is_aggregate(self):
        v = fn.Sum(Field('foo')) / fn.Sum(Field('foo'))
        self.assertTrue(v.is_aggregate)

    def test__mixed_func_arithmetic_is_not_aggregate(self):
        v = Field('foo') / fn.Sum(Field('foo'))
        self.assertFalse(v.is_aggregate)

    def test__agg_case_is_aggregate(self):
        v = Case() \
            .when(Field('foo') == 1, fn.Sum(Field('bar'))) \
            .when(Field('foo') == 2, fn.Sum(Field('fiz'))) \
            .else_(fn.Sum(Field('fiz')))

        self.assertTrue(v.is_aggregate)

    def test__mixed_case_is_not_aggregate(self):
        v = Case() \
            .when(Field('foo') == 1, fn.Sum(Field('bar'))) \
            .when(Field('foo') == 2, Field('fiz'))

        self.assertFalse(v.is_aggregate)

    def test__case_mixed_else_is_not_aggregate(self):
        v = Case() \
            .when(Field('foo') == 1, fn.Sum(Field('bar'))) \
            .when(Field('foo') == 2, fn.Sum(Field('fiz'))) \
            .else_(Field('fiz'))

        self.assertFalse(v.is_aggregate)
