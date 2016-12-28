# coding: utf-8
import unittest

from pypika import Case
from pypika import Field, functions as fn
from pypika.terms import ValueWrapper


class IsAggregateTests(unittest.TestCase):
    def test__field_is_not_aggregate(self):
        v = Field('foo')
        self.assertFalse(v.is_aggregate)

    def test__constant_is_neither_aggr_or_not(self):
        v = ValueWrapper(100)
        self.assertIsNone(v.is_aggregate)

    def test__constant_arithmetic_is_neither_aggr_or_not(self):
        v = ValueWrapper(100) + ValueWrapper(100)
        self.assertIsNone(v.is_aggregate)

    def test__field_arithmetic_is_not_aggregate(self):
        v = Field('foo') + Field('bar')
        self.assertFalse(v.is_aggregate)

    def test__field_arithmetic_constant_is_not_aggregate(self):
        v = Field('foo') + 1
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

    def test__func_arithmetic_constant_is_not_aggregate(self):
        v = 1 / fn.Sum(Field('foo'))
        self.assertTrue(v.is_aggregate)

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

    def test__case_mixed_constant_is_not_aggregate(self):
        v = Case() \
            .when(Field('foo') == 1, fn.Sum(Field('bar'))) \
            .when(Field('foo') == 2, fn.Sum(Field('fiz'))) \
            .else_(1)

        self.assertTrue(v.is_aggregate)

    def test__case_all_constants_is_neither_aggr_or_not(self):
        v = Case() \
            .when(Field('foo') == 1, 1) \
            .when(Field('foo') == 2, 2) \
            .else_(3)

        self.assertIsNone(v.is_aggregate)
