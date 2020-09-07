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


class ClickHouseDeleteTests(TestCase):
    table_abc = Table("abc")

    def test_omit_where(self):
        q = ClickHouseQuery.from_("abc").delete()

        self.assertEqual('ALTER TABLE "abc" DELETE', str(q))

    def test_omit_where__table_schema(self):
        q = ClickHouseQuery.from_(Table("abc", "schema1")).delete()

        self.assertEqual('ALTER TABLE "schema1"."abc" DELETE', str(q))

    def test_where_field_equals(self):
        q1 = (
            ClickHouseQuery.from_(self.table_abc)
            .where(self.table_abc.foo == self.table_abc.bar)
            .delete()
        )
        q2 = (
            ClickHouseQuery.from_(self.table_abc)
            .where(self.table_abc.foo.eq(self.table_abc.bar))
            .delete()
        )

        self.assertEqual('ALTER TABLE "abc" DELETE WHERE "foo"="bar"', str(q1))
        self.assertEqual('ALTER TABLE "abc" DELETE WHERE "foo"="bar"', str(q2))


class ClickHouseUpdateTests(TestCase):
    table_abc = Table("abc")

    def test_update(self):
        q = (
            ClickHouseQuery.update(self.table_abc).where(self.table_abc.foo == 0).set("foo", "bar")
        )

        self.assertEqual(
            'ALTER TABLE "abc" UPDATE "foo"=\'bar\' WHERE "foo"=0', str(q)
        )
