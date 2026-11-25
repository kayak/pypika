import sqlite3
import unittest

from pypika import Query, Table, analytics as an, functions as fn


class FunctionBuilderIsolationTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.executescript(
            "CREATE TABLE data(amount INTEGER, flag INTEGER, kind TEXT, category TEXT, rank INTEGER);"
            "INSERT INTO data VALUES (10, 1, 'x', 'a', 1), (20, 1, 'y', 'a', 1);"
        )
        self.table = Table("data")

    def values(self, expression):
        query = Query.from_(self.table).select(expression)
        return self.connection.execute(str(query)).fetchall()

    def test_filter_branches_leave_the_original_and_sibling_unchanged(self):
        base = fn.Sum(self.table.amount).filter(self.table.flag == 1)
        first = base.filter(self.table.kind == "x")
        second = base.filter(self.table.kind == "y")
        self.assertEqual(self.values(base), [(30,)])
        self.assertEqual(self.values(first), [(10,)])
        self.assertEqual(self.values(second), [(20,)])

    def test_partition_branches_leave_the_original_and_sibling_unchanged(self):
        base = an.Sum(self.table.amount).over(self.table.category)
        first = base.over(self.table.kind)
        second = base.over(self.table.flag)
        self.assertEqual(self.values(base), [(30,), (30,)])
        self.assertEqual(self.values(first), [(10,), (20,)])
        self.assertEqual(self.values(second), [(30,), (30,)])

    def test_order_branches_leave_the_original_and_sibling_unchanged(self):
        base = an.Sum(self.table.amount).orderby(self.table.rank)
        first = base.orderby(self.table.kind)
        second = base.orderby(self.table.flag)
        self.assertEqual(self.values(base), [(30,), (30,)])
        self.assertEqual(self.values(first), [(10,), (30,)])
        self.assertEqual(self.values(second), [(30,), (30,)])
