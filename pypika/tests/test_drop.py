import unittest

from pypika import (
    Columns,
    Database,
    Query,
    Tables,
)


class DropTableTests(unittest.TestCase):
    database_xyz = Database("mydb")
    new_table, existing_table = Tables("abc", "efg")
    foo, bar = Columns(("a", "INT"), ("b", "VARCHAR(100)"))

    def test_drop_database(self):
        q1 = Query.drop_database(self.database_xyz)
        q2 = Query.drop_database(self.database_xyz).if_exists()

        self.assertEqual('DROP DATABASE "mydb"', str(q1))
        self.assertEqual('DROP DATABASE IF EXISTS "mydb"', str(q2))

    def test_drop_table(self):
        q1 = Query.drop_table(self.new_table)
        q2 = Query.drop_table(self.new_table).if_exists()

        self.assertEqual('DROP TABLE "abc"', str(q1))
        self.assertEqual('DROP TABLE IF EXISTS "abc"', str(q2))

    def test_drop_user(self):
        q1 = Query.drop_user("myuser")
        q2 = Query.drop_user("myuser").if_exists()

        self.assertEqual('DROP USER "myuser"', str(q1))
        self.assertEqual('DROP USER IF EXISTS "myuser"', str(q2))

    def test_drop_view(self):
        q1 = Query.drop_view("myview")
        q2 = Query.drop_view("myview").if_exists()

        self.assertEqual('DROP VIEW "myview"', str(q1))
        self.assertEqual('DROP VIEW IF EXISTS "myview"', str(q2))
