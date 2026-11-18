import sqlite3
import unittest

from pypika import Query, Table


class BetweenTableRebindingTests(unittest.TestCase):
    def test_rebinds_both_bounds_without_mutating_original(self):
        old, new = Table("old"), Table("new")
        criterion = old.value.between(old.low, old.high).as_("in_range")
        original = criterion.get_sql(with_namespace=True, quote_char='"')
        replaced = criterion.replace_table(old, new)

        self.assertEqual(
            '"new"."value" BETWEEN "new"."low" AND "new"."high" "in_range"',
            replaced.get_sql(with_namespace=True, quote_char='"'),
        )
        self.assertEqual(original, criterion.get_sql(with_namespace=True, quote_char='"'))
        self.assertEqual("in_range", replaced.alias)

    def test_preserves_unrelated_bound_table_and_literal(self):
        old, new, other = Table("old"), Table("new"), Table("other")
        criterion = old.value.between(other.low + old.offset, 10)
        replaced = criterion.replace_table(old, new)
        self.assertEqual(
            '"new"."value" BETWEEN "other"."low"+"new"."offset" AND 10',
            replaced.get_sql(with_namespace=True, quote_char='"'),
        )

    def test_rebound_query_runs_against_sqlite(self):
        old, new = Table("old", alias="s"), Table("new", alias="d")
        criterion = old.value.between(old.low, old.high)
        query = Query.from_(old).select(old.value).where(criterion)
        rebound = query.replace_table(old, new)
        with sqlite3.connect(":memory:") as conn:
            conn.execute('CREATE TABLE "old" (value, low, high)')
            conn.execute('CREATE TABLE "new" (value, low, high)')
            for table in ("old", "new"):
                conn.executemany(f'INSERT INTO "{table}" VALUES (?, ?, ?)', [(5, 1, 9), (20, 1, 9)])
            self.assertEqual(conn.execute(str(query)).fetchall(), [(5,)])
            self.assertEqual(conn.execute(str(rebound)).fetchall(), [(5,)])


if __name__ == "__main__":
    unittest.main()
