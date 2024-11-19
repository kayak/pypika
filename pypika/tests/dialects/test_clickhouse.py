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

    def test_use_FINAL_keyword(self):
        t = Table('abc')
        query = ClickHouseQuery.from_(t).select(t.foo).final()
        self.assertEqual(str(query), 'SELECT "foo" FROM "abc" FINAL')


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
