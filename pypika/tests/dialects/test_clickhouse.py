from unittest import TestCase

from pypika import (
    ClickHouseQuery,
    Table,
)


class ClickHouseQueryTests(TestCase):
    def test_use_AS_keyword_for_alias(self):
        t = Table('abc')
        query = ClickHouseQuery.from_(t).select(t.foo.as_('f1'), t.bar.as_('f2'))
        self.assertEqual(str(query), 'SELECT "foo" AS "f1","bar" AS "f2" FROM "abc"')
