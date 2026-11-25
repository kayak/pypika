import sqlite3
import unittest

from pypika import Query, Table, functions as fn


class NegativeOperandTests(unittest.TestCase):
    def test_query_table_replacement_reaches_a_negative_select(self):
        source = Table("source").as_("s")
        destination = Table("destination").as_("d")
        original = Query.from_(source).select((-source.amount).as_("negative_amount"))
        original_sql = str(original)
        replaced = original.replace_table(source, destination)

        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE destination (amount INTEGER)")
            connection.execute("INSERT INTO destination VALUES (7)")
            cursor = connection.execute(str(replaced))
            self.assertEqual(cursor.fetchone(), (-7,))
            self.assertEqual(cursor.description[0][0], "negative_amount")
        self.assertEqual(str(original), original_sql)

    def test_nested_operand_fields_are_discoverable_and_replaceable(self):
        source = Table("source").as_("s")
        destination = Table("destination").as_("d")
        expression = -(source.amount + fn.Coalesce(source.adjustment, 0))
        self.assertEqual(expression.fields_(), {source.amount, source.adjustment})

        replaced = expression.replace_table(source, destination)
        self.assertEqual(replaced.fields_(), {destination.amount, destination.adjustment})
        self.assertEqual(expression.fields_(), {source.amount, source.adjustment})
