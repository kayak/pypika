import sqlite3
import unittest

from pypika import Field, MySQLQuery, Query, Table
from pypika.terms import BitwiseAndCriterion, BitwiseOrCriterion


class BitwiseOperandTests(unittest.TestCase):
    def test_joined_right_field_uses_its_table(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.executescript(
            "CREATE TABLE numbers(id INTEGER, bits INTEGER);"
            "CREATE TABLE masks(id INTEGER, bits INTEGER);"
            "INSERT INTO numbers VALUES (1, 10);"
            "INSERT INTO masks VALUES (1, 6);"
        )
        numbers, masks = Table("numbers"), Table("masks")
        for method, expected in (("bitwiseand", 2), ("bitwiseor", 14)):
            with self.subTest(method=method):
                expression = getattr(numbers.bits, method)(masks.bits)
                query = Query.from_(numbers).join(masks).on(numbers.id == masks.id).select(expression)
                self.assertEqual(connection.execute(str(query)).fetchall(), [(expected,)])

    def test_right_field_uses_the_dialect_quotes(self):
        table = Table("data")
        for method, operator in (("bitwiseand", "&"), ("bitwiseor", "|")):
            with self.subTest(method=method):
                query = MySQLQuery.from_(table).select(getattr(table.bits, method)(table.mask.as_("ignored")))
                self.assertEqual(str(query), "SELECT (`bits` " + operator + " `mask`) FROM `data`")

    def test_replacement_includes_the_right_operand(self):
        old, new, left = Table("old"), Table("new"), Table("left")
        for method in ("bitwiseand", "bitwiseor"):
            with self.subTest(method=method):
                original = getattr(left.bits, method)(old.bits)
                replaced = original.replace_table(old, new)
                self.assertEqual(replaced.tables_, {left, new})
                self.assertEqual(original.tables_, {left, old})

    def test_direct_criteria_accept_constant_operands(self):
        for criterion, operator in ((BitwiseAndCriterion, "&"), (BitwiseOrCriterion, "|")):
            with self.subTest(criterion=criterion):
                field = Field("bits")
                expression = criterion(field, 3)
                self.assertEqual(expression.fields_(), {field})
                self.assertEqual(expression.get_sql(), "(bits " + operator + " 3)")
