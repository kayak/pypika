import contextlib
import sqlite3
import unittest

from pypika import Query, Table, analytics


class ZeroWindowBoundaryTests(unittest.TestCase):
    def test_zero_and_unbounded_edges(self):
        for edge in (analytics.Preceding, analytics.Following):
            with self.subTest(edge=edge):
                self.assertEqual("0 " + edge.modifier, str(edge(0)))
                self.assertEqual("UNBOUNDED " + edge.modifier, str(edge()))

    def test_current_row_window_results(self):
        table = Table("items")
        for frame in ("rows", "range"):
            total = analytics.Sum(table.amount).orderby(table.amount)
            total = getattr(total, frame)(analytics.Preceding(0), analytics.Following(0))
            query = Query.from_(table).select(table.amount, total).orderby(table.amount)
            with self.subTest(frame=frame), contextlib.closing(sqlite3.connect(":memory:")) as connection:
                connection.execute("CREATE TABLE items (amount INTEGER)")
                connection.executemany("INSERT INTO items VALUES (?)", [(1,), (2,), (4,)])
                self.assertEqual([(1, 1), (2, 2), (4, 4)], connection.execute(str(query)).fetchall())
