import unittest

from pypika import Column, MySQLQuery, QueryException, Table


class SelectTests(unittest.TestCase):
    table_abc = Table("abc")

    def test_normal_select(self):
        q = MySQLQuery.from_("abc").select("def")

        self.assertEqual("SELECT `def` FROM `abc`", str(q))

    def test_distinct_select(self):
        q = MySQLQuery.from_("abc").select("def").distinct()

        self.assertEqual("SELECT DISTINCT `def` FROM `abc`", str(q))

    def test_modifier_select(self):
        q = MySQLQuery.from_("abc").select("def").select("ghi").modifier("SQL_CALC_FOUND_ROWS")

        self.assertEqual("SELECT SQL_CALC_FOUND_ROWS `def`,`ghi` FROM `abc`", str(q))

    def test_multiple_modifier_select(self):
        q = MySQLQuery.from_("abc").select("def").modifier("HIGH_PRIORITY").modifier("SQL_CALC_FOUND_ROWS")

        self.assertEqual("SELECT HIGH_PRIORITY SQL_CALC_FOUND_ROWS `def` FROM `abc`", str(q))


class UpdateTests(unittest.TestCase):
    table_abc = Table("abc")

    def test_update(self):
        q = MySQLQuery.into("abc").insert(1, [1, "a", True])

        self.assertEqual("INSERT INTO `abc` VALUES (1,[1,'a',true])", str(q))

    def test_on_duplicate_key_ignore_update(self):
        q = MySQLQuery.into("abc").insert(1, [1, "a", True]).on_duplicate_key_ignore()

        self.assertEqual("INSERT INTO `abc` VALUES (1,[1,'a',true]) ON DUPLICATE KEY IGNORE", str(q))

    def test_on_duplicate_key_update_update(self):
        q = MySQLQuery.into("abc").insert(1, [1, "a", True]).on_duplicate_key_update(self.table_abc.a, 'b')

        self.assertEqual("INSERT INTO `abc` VALUES (1,[1,'a',true]) ON DUPLICATE KEY UPDATE `a`='b'", str(q))

    def test_conflict_handlers_update(self):
        with self.assertRaises(QueryException):
            (
                MySQLQuery.into("abc")
                .insert(1, [1, "a", True])
                .on_duplicate_key_ignore()
                .on_duplicate_key_update(self.table_abc.a, 'b')
            )


class LoadCSVTests(unittest.TestCase):
    table_abc = Table("abc")

    def test_load_from_file(self):
        q1 = MySQLQuery.load("/path/to/file").into("abc")

        q2 = MySQLQuery.load("/path/to/file").into(self.table_abc)

        self.assertEqual(
            "LOAD DATA LOCAL INFILE '/path/to/file' INTO TABLE `abc` FIELDS TERMINATED BY ','",
            str(q1),
        )
        self.assertEqual(
            "LOAD DATA LOCAL INFILE '/path/to/file' INTO TABLE `abc` FIELDS TERMINATED BY ','",
            str(q2),
        )


class TableTests(unittest.TestCase):
    table_abc = Table("abc")

    def test_create_table(self):
        q = MySQLQuery.create_table(self.table_abc).columns(Column("id", "INT"))
        self.assertEqual(
            'CREATE TABLE `abc` (`id` INT)',
            str(q),
        )

    def test_drop_table(self):
        q = MySQLQuery.drop_table(self.table_abc)
        self.assertEqual(
            'DROP TABLE `abc`',
            str(q),
        )
