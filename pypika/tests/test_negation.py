import sqlite3
import unittest

from pypika import (
    Query,
    Table,
    Tables,
)
from pypika import (
    functions as fn,
)
from pypika.terms import LiteralValue, ValueWrapper


class NegationTests(unittest.TestCase):
    table_abc, table_efg = Tables("abc", "efg")

    def test_negate_wrapped_float(self):
        q = -ValueWrapper(1.0)

        self.assertEqual("-1.0", q.get_sql())

    def test_negate_wrapped_int(self):
        q = -ValueWrapper(1)

        self.assertEqual("-1", q.get_sql())

    def test_negate_field(self):
        q = -self.table_abc.foo

        self.assertEqual('-"abc"."foo"', q.get_sql(with_namespace=True, quote_char='"'))

    def test_negate_function(self):
        q = -fn.Sum(self.table_abc.foo)

        self.assertEqual('-SUM("abc"."foo")', q.get_sql(with_namespace=True, quote_char='"'))


class NegativeExpressionTests(unittest.TestCase):
    def test_expression_roundtrip(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE numbers (a INTEGER, b INTEGER)")
        connection.execute("INSERT INTO numbers VALUES (7, 3)")
        t = Table("numbers")
        cases = [
            ("sum", -(t.a + t.b), '-("a"+"b")', -10),
            ("difference", -(t.a - t.b), '-("a"-"b")', -4),
            ("multiply", (-(t.a + t.b)) * 2, '-("a"+"b")*2', -20),
            ("divide_left", (-(t.a + t.b)) / 2, '-("a"+"b")/2', -5),
            ("divide_right", 20 / (-(t.a + t.b)), '20/-("a"+"b")', -2),
            ("double_negative", -(-t.a), '-(-"a")', 7),
            ("triple_negative", -(-(-t.a)), '-(-(-"a"))', -7),
            ("negative_literal", -ValueWrapper(-7), '-(-7)', 7),
            ("operand_alias", -((t.a + t.b).as_("total")), '-("a"+"b") "total"', -10),
            ("result_alias", (-(t.a + t.b)).as_("total"), '-("a"+"b") "total"', -10),
            ("positive_literal_alias", -ValueWrapper(7).as_("total"), '-7 "total"', -7),
            ("negative_literal_alias", -ValueWrapper(-7).as_("total"), '-(-7) "total"', 7),
            ("raw_literal_alias", -LiteralValue("7").as_("total"), '-7 "total"', -7),
        ]
        for name, expression, expected_sql, expected_value in cases:
            with self.subTest(name=name):
                sql = str(Query.from_(t).select(expression))
                cursor = connection.execute(sql)
                self.assertEqual(expected_value, cursor.fetchone()[0])
                self.assertEqual('SELECT {} FROM "numbers"'.format(expected_sql), sql)
                if name.endswith("alias"):
                    self.assertEqual("total", cursor.description[0][0])

    def test_compound_operand_quoting_and_namespace(self):
        table = Table("numbers")
        expression = -(table.a + table.b)

        self.assertEqual('-("numbers"."a"+"numbers"."b")', expression.get_sql(with_namespace=True, quote_char='"'))
        self.assertEqual('-(a+b)', expression.get_sql())

    def test_simple_operand_alias_is_preserved(self):
        table = Table("numbers")

        self.assertEqual('SELECT -"a" "total" FROM "numbers"', str(Query.from_(table).select(-table.a.as_("total"))))

    def test_result_alias_overrides_operand_without_mutating_it(self):
        operand = ValueWrapper(-7).as_("inner")
        expression = -operand

        self.assertEqual('-(-7) "outer"', expression.as_("outer").get_sql(with_alias=True, quote_char='"'))
        self.assertEqual('-(-7) ""', expression.as_("").get_sql(with_alias=True, quote_char='"'))
        self.assertEqual("inner", operand.alias)
        self.assertIsNone(expression.alias)
