import operator
import sqlite3
import unittest
from itertools import product

from pypika import Query, Table
from pypika.terms import Term


class ShiftGroupingTests(unittest.TestCase):
    def assert_result(self, expression, expected):
        with sqlite3.connect(":memory:") as connection:
            actual = connection.execute(str(Query.select(expression))).fetchone()[0]
        self.assertEqual(expected, actual)

    def test_shift_on_right_of_addition(self):
        value = Term.wrap_constant
        self.assert_result(value(1) + (value(2) << 1), 5)

    def test_shift_on_left_of_addition(self):
        value = Term.wrap_constant
        self.assert_result((value(1) << 2) + 1, 5)

    def test_shift_on_right_of_another_shift(self):
        value = Term.wrap_constant
        self.assert_result(value(16) >> (value(1) << 1), 4)

    def test_nested_shift_expression_keeps_alias_and_source_unchanged(self):
        table = Table("values_table")
        expression = table.a + (table.b << table.c)
        original = expression.get_sql()
        aliased = expression.as_("result")
        query = Query.from_(table).select(aliased)
        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE values_table (a INTEGER, b INTEGER, c INTEGER)")
            connection.execute("INSERT INTO values_table VALUES (1, 2, 1)")
            cursor = connection.execute(str(query))
            self.assertEqual(cursor.description[0][0], "result")
            self.assertEqual(cursor.fetchone(), (5,))
        self.assertEqual(expression.get_sql(), original)
        self.assertIsNone(expression.alias)

    def test_nested_operators_match_fully_parenthesized_sql(self):
        operations = {
            "+": operator.add,
            "-": operator.sub,
            "*": operator.mul,
            "<<": operator.lshift,
            ">>": operator.rshift,
        }
        values = ((1, 2, 1), (16, 1, 1), (3, 2, 2), (7, 1, 2), (0, 2, 0))
        with sqlite3.connect(":memory:") as connection:
            for (outer_symbol, outer), (inner_symbol, inner), (a, b, c) in product(
                operations.items(), operations.items(), values
            ):
                if outer_symbol not in {"<<", ">>"} and inner_symbol not in {"<<", ">>"}:
                    continue
                left = outer(inner(Term.wrap_constant(a), Term.wrap_constant(b)), Term.wrap_constant(c))
                right = outer(Term.wrap_constant(a), inner(Term.wrap_constant(b), Term.wrap_constant(c)))
                for expression, reference in (
                    (left, f"SELECT (({a} {inner_symbol} {b}) {outer_symbol} {c})"),
                    (right, f"SELECT ({a} {outer_symbol} ({b} {inner_symbol} {c}))"),
                ):
                    with self.subTest(outer=outer_symbol, inner=inner_symbol, values=(a, b, c), reference=reference):
                        expected = connection.execute(reference).fetchone()[0]
                        actual = connection.execute(str(Query.select(expression))).fetchone()[0]
                        self.assertEqual(expected, actual)

    def test_non_shift_and_simple_shift_rendering_is_unchanged(self):
        value = Term.wrap_constant
        cases = (
            (value(2) << 1, "2<<1"),
            (value(8) >> 1, "8>>1"),
            (value(1) + value(2) * 3, "1+2*3"),
            ((value(1) + 2) * 3, "(1+2)*3"),
            (value(8) / (value(2) * 2), "8/(2*2)"),
        )
        for expression, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(expression.get_sql(), expected)
