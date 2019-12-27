import unittest

from pypika import Table, Query, PostgreSQLQuery

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class UpdateTests(unittest.TestCase):
    table_abc = Table("abc")
    table_def = Table("def")

    def test_empty_query(self):
        q = Query.update("abc")

        self.assertEqual("", str(q))

    def test_omit_where(self):
        q = Query.update(self.table_abc).set("foo", "bar")

        self.assertEqual('UPDATE "abc" SET "foo"=\'bar\'', str(q))

    def test_single_quote_escape_in_set(self):
        q = Query.update(self.table_abc).set("foo", "bar'foo")

        self.assertEqual("UPDATE \"abc\" SET \"foo\"='bar''foo'", str(q))

    def test_update__table_schema(self):
        table = Table("abc", "schema1")
        q = Query.update(table).set(table.foo, 1).where(table.foo == 0)

        self.assertEqual('UPDATE "schema1"."abc" SET "foo"=1 WHERE "foo"=0', str(q))

    def test_update_with_none(self):
        q = Query.update("abc").set("foo", None)
        self.assertEqual('UPDATE "abc" SET "foo"=null', str(q))

    def test_update_with_join(self):
        q = (
            Query.update(self.table_abc)
            .join(self.table_def)
            .on(self.table_def.abc_id == self.table_abc.id)
            .set(self.table_abc.lname, self.table_def.lname)
        )
        self.assertEqual(
            'UPDATE "abc" JOIN "def" ON "def"."abc_id"="abc"."id" SET "abc"."lname"="def"."lname"',
            str(q),
        )

    def test_update_with_limit(self):
        q = Query.update(self.table_abc).set(self.table_abc.lname, "test").limit(1)
        self.assertEqual('UPDATE "abc" SET "lname"=\'test\' LIMIT 1', str(q))


class PostgresUpdateTests(unittest.TestCase):
    table_abc = Table("abc")

    def test_update_returning_str(self):
        q = (
            PostgreSQLQuery.update(self.table_abc)
            .where(self.table_abc.foo == 0)
            .set("foo", "bar")
            .returning("id")
        )

        self.assertEqual(
            'UPDATE "abc" SET "foo"=\'bar\' WHERE "foo"=0 RETURNING id', str(q)
        )

    def test_update_returning(self):
        q = (
            PostgreSQLQuery.update(self.table_abc)
            .where(self.table_abc.foo == 0)
            .set("foo", "bar")
            .returning(self.table_abc.id)
        )

        self.assertEqual(
            'UPDATE "abc" SET "foo"=\'bar\' WHERE "foo"=0 RETURNING id', str(q)
        )
