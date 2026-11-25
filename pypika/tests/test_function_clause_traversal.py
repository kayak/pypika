import sqlite3
import unittest

from pypika import Query, Table, analytics as an, functions as fn
from pypika.terms import ValueWrapper


class FunctionClauseTraversalTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.executescript(
            "CREATE TABLE events(amount INTEGER, threshold INTEGER, bucket TEXT);"
            "CREATE TABLE settings(threshold INTEGER, bucket TEXT);"
            "INSERT INTO events VALUES (10, 100, 'a'), (20, 100, 'b');"
            "INSERT INTO settings VALUES (15, 'one');"
        )
        self.events, self.settings = Table("events"), Table("settings")

    def query_for_expression(self, expression):
        query = Query.from_(self.events).select(expression)
        for table in expression.tables_ - {self.events}:
            query = query.from_(table)
        return query

    def test_filter_table_can_be_discovered_for_query_construction(self):
        expression = fn.Count("*").filter(self.events.amount >= self.settings.threshold)
        query = self.query_for_expression(expression)
        self.assertEqual(self.connection.execute(str(query)).fetchall(), [(1,)])

    def test_partition_table_can_be_discovered_for_query_construction(self):
        expression = an.Sum(self.events.amount).over(self.settings.bucket)
        query = self.query_for_expression(expression)
        self.assertEqual(self.connection.execute(str(query)).fetchall(), [(30,), (30,)])

    def test_fields_include_all_function_clauses(self):
        expression = (
            an.Sum(self.events.amount)
            .filter(self.events.amount >= self.settings.threshold)
            .over(self.settings.bucket)
            .orderby(self.settings.threshold)
        )
        self.assertEqual(
            expression.fields_(),
            {self.events.amount, self.settings.threshold, self.settings.bucket},
        )
        self.assertEqual(expression.tables_, {self.events, self.settings})

    def test_non_node_partition_entries_remain_supported(self):
        expression = an.Sum(self.events.amount).over("1").orderby(ValueWrapper(2))
        self.assertEqual(expression.fields_(), {self.events.amount})
        self.assertEqual(an.Rank().over().fields_(), set())
