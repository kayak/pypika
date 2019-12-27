import unittest

from pypika import (
    Tables,
    functions as fn,
)
from pypika.dialects import SnowflakeQuery


class QuoteTests(unittest.TestCase):
    table_abc, table_efg = Tables("abc", "efg")

    def test_use_double_quotes_on_alias_but_not_on_terms(self):
        q = SnowflakeQuery.from_(self.table_abc).select(self.table_abc.a.as_("bar"))

        self.assertEqual('SELECT a "bar" ' "FROM abc", q.get_sql(with_namespace=True))

    def test_use_double_quotes_on_alias_but_not_on_terms_with_joins(self):
        foo = self.table_abc.as_("foo")
        bar = self.table_efg.as_("bar")

        q = (
            SnowflakeQuery.from_(foo)
            .join(bar)
            .on(foo.fk == bar.id)
            .select(foo.a, bar.b)
        )

        self.assertEqual(
            "SELECT foo.a,bar.b "
            'FROM abc "foo" '
            'JOIN efg "bar" '
            "ON foo.fk=bar.id",
            q.get_sql(with_namespace=True),
        )

    def test_use_double_quotes_on_alias_but_not_on_terms(self):
        idx = self.table_abc.index.as_("idx")
        val = fn.Sum(self.table_abc.value).as_("val")
        q = (
            SnowflakeQuery.from_(self.table_abc)
            .select(idx, val)
            .groupby(idx)
            .orderby(idx)
        )

        self.assertEqual(
            'SELECT index "idx",SUM(value) "val" '
            "FROM abc "
            'GROUP BY "idx" '
            'ORDER BY "idx"',
            q.get_sql(with_namespace=True),
        )
