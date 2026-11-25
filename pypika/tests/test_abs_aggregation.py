import unittest

from pypika import Field, functions


class AbsAggregationTests(unittest.TestCase):
    def test_scalar_arguments_are_not_aggregates(self):
        self.assertIs(False, functions.Abs(Field("amount")).is_aggregate)
        self.assertIs(None, functions.Abs(-2).is_aggregate)

    def test_aggregate_argument_stays_aggregated(self):
        expression = functions.Abs(functions.Sum(Field("amount")))
        self.assertIs(True, expression.is_aggregate)
        self.assertEqual('ABS(SUM("amount"))', str(expression))

    def test_scalar_abs_in_arithmetic(self):
        expression = functions.Abs(Field("amount")) + functions.Abs(Field("tax"))
        self.assertIs(False, expression.is_aggregate)
