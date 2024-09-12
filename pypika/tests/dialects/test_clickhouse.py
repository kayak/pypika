from unittest import TestCase

from pypika import (
    ClickHouseQuery,
    Database,
    Table,
)


class ClickHouseQueryTests(TestCase):
    def test_use_AS_keyword_for_alias(self):
        t = Table('abc')
        query = ClickHouseQuery.from_(t).select(t.foo.as_('f1'), t.bar.as_('f2'))
        self.assertEqual(str(query), 'SELECT "foo" AS "f1","bar" AS "f2" FROM "abc"')

    def test_use_SAMPLE_keyword(self):
        t = Table('abc')
        query = ClickHouseQuery.from_(t).select(t.foo).sample(10)
        self.assertEqual(str(query), 'SELECT "foo" FROM "abc" SAMPLE 10')

    def test_use_SAMPLE_with_offset_keyword(self):
        t = Table('abc')
        query = ClickHouseQuery.from_(t).select(t.foo).sample(10, 5)
        self.assertEqual(str(query), 'SELECT "foo" FROM "abc" SAMPLE 10 OFFSET 5')


class ClickHouseDeleteTests(TestCase):
    table_abc = Table("abc")

    def test_omit_where(self):
        q = ClickHouseQuery.from_("abc").delete()

        self.assertEqual('ALTER TABLE "abc" DELETE', str(q))

    def test_omit_where__table_schema(self):
        q = ClickHouseQuery.from_(Table("abc", "schema1")).delete()

        self.assertEqual('ALTER TABLE "schema1"."abc" DELETE', str(q))

    def test_where_field_equals(self):
        q1 = ClickHouseQuery.from_(self.table_abc).where(self.table_abc.foo == self.table_abc.bar).delete()
        q2 = ClickHouseQuery.from_(self.table_abc).where(self.table_abc.foo.eq(self.table_abc.bar)).delete()

        self.assertEqual('ALTER TABLE "abc" DELETE WHERE "foo"="bar"', str(q1))
        self.assertEqual('ALTER TABLE "abc" DELETE WHERE "foo"="bar"', str(q2))


class ClickHouseUpdateTests(TestCase):
    table_abc = Table("abc")

    def test_update(self):
        q = ClickHouseQuery.update(self.table_abc).where(self.table_abc.foo == 0).set("foo", "bar")

        self.assertEqual('ALTER TABLE "abc" UPDATE "foo"=\'bar\' WHERE "foo"=0', str(q))


class ClickHouseDropQuery(TestCase):
    table_abc = Table("abc")
    database_xyz = Database("mydb")
    cluster_name = "mycluster"

    def test_drop_database(self):
        q1 = ClickHouseQuery.drop_database(self.database_xyz)
        q2 = ClickHouseQuery.drop_database(self.database_xyz).on_cluster(self.cluster_name)
        q3 = ClickHouseQuery.drop_database(self.database_xyz).if_exists().on_cluster(self.cluster_name)

        self.assertEqual('DROP DATABASE "mydb"', str(q1))
        self.assertEqual('DROP DATABASE "mydb" ON CLUSTER "mycluster"', str(q2))
        self.assertEqual('DROP DATABASE IF EXISTS "mydb" ON CLUSTER "mycluster"', str(q3))

    def test_drop_table(self):
        q1 = ClickHouseQuery.drop_table(self.table_abc)
        q2 = ClickHouseQuery.drop_table(self.table_abc).on_cluster(self.cluster_name)
        q3 = ClickHouseQuery.drop_table(self.table_abc).if_exists().on_cluster(self.cluster_name)

        self.assertEqual('DROP TABLE "abc"', str(q1))
        self.assertEqual('DROP TABLE "abc" ON CLUSTER "mycluster"', str(q2))
        self.assertEqual('DROP TABLE IF EXISTS "abc" ON CLUSTER "mycluster"', str(q3))

    def test_drop_dictionary(self):
        q1 = ClickHouseQuery.drop_dictionary("dict")
        q2 = ClickHouseQuery.drop_dictionary("dict").on_cluster(self.cluster_name)
        q3 = ClickHouseQuery.drop_dictionary("dict").if_exists().on_cluster(self.cluster_name)

        self.assertEqual('DROP DICTIONARY "dict"', str(q1))
        self.assertEqual('DROP DICTIONARY "dict"', str(q2))  # NO CLUSTER
        self.assertEqual('DROP DICTIONARY IF EXISTS "dict"', str(q3))  # NO CLUSTER

    def test_drop_other(self):
        q1 = ClickHouseQuery.drop_quota("myquota")
        q2 = ClickHouseQuery.drop_user("myuser")
        q3 = ClickHouseQuery.drop_view("myview")

        self.assertEqual('DROP QUOTA "myquota"', str(q1))
        self.assertEqual('DROP USER "myuser"', str(q2))
        self.assertEqual('DROP VIEW "myview"', str(q3))


class DistinctOnTests(TestCase):
    table_abc = Table("abc")

    def test_distinct_on(self):
        q = ClickHouseQuery.from_(self.table_abc).distinct_on("lname", self.table_abc.fname).select("lname", "id")

        self.assertEqual('''SELECT DISTINCT ON("lname","fname") "lname","id" FROM "abc"''', str(q))


class LimitByTests(TestCase):
    table_abc = Table("abc")

    def test_limit_by(self):
        q = ClickHouseQuery.from_(self.table_abc).limit_by(1, "a", self.table_abc.b).select("a", "b", "c")

        self.assertEqual('''SELECT "a","b","c" FROM "abc" LIMIT 1 BY ("a","b")''', str(q))

    def test_limit_offset_by(self):
        q = ClickHouseQuery.from_(self.table_abc).limit_offset_by(1, 2, "a", self.table_abc.b).select("a", "b", "c")

        self.assertEqual('''SELECT "a","b","c" FROM "abc" LIMIT 1 OFFSET 2 BY ("a","b")''', str(q))

    def test_limit_offset0_by(self):
        q = ClickHouseQuery.from_(self.table_abc).limit_offset_by(1, 0, "a", self.table_abc.b).select("a", "b", "c")

        self.assertEqual('''SELECT "a","b","c" FROM "abc" LIMIT 1 BY ("a","b")''', str(q))

    def test_rename_table(self):
        table_join = Table("join")

        q = (
            ClickHouseQuery.from_(self.table_abc)
            .join(table_join)
            .using("a")
            .limit_by(1, self.table_abc.a, table_join.a)
            .select(self.table_abc.b, table_join.b)
        )
        q = q.replace_table(self.table_abc, Table("xyz"))

        self.assertEqual(
            '''SELECT "xyz"."b","join"."b" FROM "xyz" JOIN "join" USING ("a") LIMIT 1 BY ("xyz"."a","join"."a")''',
            str(q),
        )
