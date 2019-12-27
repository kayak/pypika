import unittest

from pypika import (
    Case,
    Field,
    Table,
    functions as fn,
)
from pypika.terms import (
    Negative,
    ValueWrapper,
)


class IsAggregateTests(unittest.TestCase):
    def test__field_is_not_aggregate(self):
        v = Field("foo")
        self.assertFalse(v.is_aggregate)

    def test__constant_is_aggregate_none(self):
        v = ValueWrapper(100)
        self.assertIsNone(v.is_aggregate)

    def test__constant_arithmetic_is_aggregate_none(self):
        v = ValueWrapper(100) + ValueWrapper(100)
        self.assertIsNone(v.is_aggregate)

    def test__field_arithmetic_is_not_aggregate(self):
        v = Field("foo") + Field("bar")
        self.assertFalse(v.is_aggregate)

    def test__field_arithmetic_constant_is_not_aggregate(self):
        v = Field("foo") + 1
        self.assertFalse(v.is_aggregate)

    def test__agg_func_is_aggregate(self):
        v = fn.Sum(Field("foo"))
        self.assertTrue(v.is_aggregate)

    def test__negative_agg_func_is_aggregate(self):
        v = Negative(fn.Sum(Field("foo")))
        self.assertTrue(v.is_aggregate)

    def test__agg_func_arithmetic_is_aggregate(self):
        v = fn.Sum(Field("foo")) / fn.Sum(Field("foo"))
        self.assertTrue(v.is_aggregate)

    def test__mixed_func_arithmetic_is_not_aggregate(self):
        v = Field("foo") / fn.Sum(Field("foo"))
        self.assertFalse(v.is_aggregate)

    def test__func_arithmetic_constant_is_not_aggregate(self):
        v = 1 / fn.Sum(Field("foo"))
        self.assertTrue(v.is_aggregate)

    def test__agg_case_is_aggregate(self):
        v = (
            Case()
            .when(Field("foo") == 1, fn.Sum(Field("bar")))
            .when(Field("foo") == 2, fn.Sum(Field("fiz")))
            .else_(fn.Sum(Field("fiz")))
        )

        self.assertTrue(v.is_aggregate)

    def test__mixed_case_is_not_aggregate(self):
        v = (
            Case()
            .when(Field("foo") == 1, fn.Sum(Field("bar")))
            .when(Field("foo") == 2, Field("fiz"))
        )

        self.assertFalse(v.is_aggregate)

    def test__case_mixed_else_is_not_aggregate(self):
        v = (
            Case()
            .when(Field("foo") == 1, fn.Sum(Field("bar")))
            .when(Field("foo") == 2, fn.Sum(Field("fiz")))
            .else_(Field("fiz"))
        )

        self.assertFalse(v.is_aggregate)

    def test__case_mixed_constant_is_not_aggregate(self):
        v = (
            Case()
            .when(Field("foo") == 1, fn.Sum(Field("bar")))
            .when(Field("foo") == 2, fn.Sum(Field("fiz")))
            .else_(1)
        )

        self.assertTrue(v.is_aggregate)

    def test__case_with_field_is_not_aggregate(self):
        v = Case().when(Field("foo") == 1, 1).when(Field("foo") == 2, 2).else_(3)

        self.assertFalse(v.is_aggregate)

    def test__case_with_single_aggregate_field_is_not_aggregate(self):
        v = (
            Case()
            .when(Field("foo") == 1, 1)
            .when(fn.Sum(Field("foo")) == 2, 2)
            .else_(3)
        )

        self.assertFalse(v.is_aggregate)

    def test__case_all_constants_is_aggregate_none(self):
        v = Case().when(True, 1).when(False, 2).else_(3)

        self.assertIsNone(v.is_aggregate)

    def test__non_aggregate_function_with_aggregated_arg(self):
        t = Table("abc")
        expr = fn.Sqrt(fn.Sum(t.a))

        self.assertTrue(expr.is_aggregate)

    def test_complicated(self):
        t = Table("abc")
        is_placebo = t.campaign_extra_info == "placebo"

        pixel_mobile_search = Case().when(
            is_placebo, t.action_fb_pixel_search + t.action_fb_mobile_search
        )
        unique_impressions = Case().when(is_placebo, t.unique_impressions)

        v = fn.Sum(pixel_mobile_search) / fn.Sum(unique_impressions) - 1.96 * fn.Sqrt(
            1
            / fn.Sum(unique_impressions)
            * fn.Sum(pixel_mobile_search)
            / fn.Sum(unique_impressions)
            * (1 - fn.Sum(pixel_mobile_search) / fn.Sum(unique_impressions))
        )

        self.assertTrue(v.is_aggregate)
