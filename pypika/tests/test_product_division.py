import sqlite3
import unittest

from pypika import Query, Table
from pypika.terms import Term


class ProductDivisionTests(unittest.TestCase):
    def test_product_of_integer_division_matches_explicit_grouping(self):
        with sqlite3.connect(":memory:") as connection:
            for a, b, c in [(2, 3, 2), (3, 5, 2), (-2, 3, 2), (2, -3, 2), (2, 0, 3)]:
                with self.subTest(values=(a, b, c)):
                    expression = Term.wrap_constant(a) * (Term.wrap_constant(b) / c)
                    expected = connection.execute("SELECT ? * (? / ?)", (a, b, c)).fetchone()
                    actual = connection.execute(str(Query.select(expression))).fetchone()
                    self.assertEqual(actual, expected)

    def test_field_expression_preserves_its_result_alias(self):
        table = Table("numbers")
        expression = table.a * (table.b / table.c)
        query = Query.from_(table).select(expression.as_("product"))
        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE numbers (a INTEGER, b INTEGER, c INTEGER)")
            connection.execute("INSERT INTO numbers VALUES (2, 3, 2)")
            cursor = connection.execute(str(query))
            self.assertEqual(cursor.fetchone(), (2,))
            self.assertEqual(cursor.description[0][0], "product")
        self.assertIsNone(expression.alias)

    def test_left_division_remains_left_associative(self):
        value = Term.wrap_constant
        self.assertEqual((value(3) / 2 * 2).get_sql(), "3/2*2")
        with sqlite3.connect(":memory:") as connection:
            query = Query.select(value(2) * (value(3.0) / 2))
            self.assertEqual(connection.execute(str(query)).fetchone(), (3.0,))
