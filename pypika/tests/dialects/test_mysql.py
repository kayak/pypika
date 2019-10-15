import unittest

from pypika import Table
from pypika.dialects import MySQLQuery


class SelectTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_normal_select(self):
        q = MySQLQuery.from_('abc').select('def')

        self.assertEqual('SELECT `def` FROM `abc`', str(q))

    def test_distinct_select(self):
        q = MySQLQuery.from_('abc').select('def').distinct()

        self.assertEqual('SELECT DISTINCT `def` FROM `abc`', str(q))

    def test_modifier_select(self):
        q = MySQLQuery.from_('abc').select('def').select('ghi').modifier('SQL_CALC_FOUND_ROWS')

        self.assertEqual('SELECT SQL_CALC_FOUND_ROWS `def`,`ghi` FROM `abc`', str(q))

    def test_multiple_modifier_select(self):
        q = MySQLQuery.from_('abc').select('def').modifier('HIGH_PRIORITY').modifier('SQL_CALC_FOUND_ROWS')

        self.assertEqual('SELECT HIGH_PRIORITY SQL_CALC_FOUND_ROWS `def` FROM `abc`', str(q))


class LoadCSVTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_load_from_file(self):
        fp = '/path/to/file'
        q = MySQLQuery \
            .load(fp) \
            .into(self.table_abc)

        self.assertEqual('LOAD DATA LOCAL INFILE \'/path/to/file\' INTO TABLE `abc` FIELDS TERMINATED BY \',\'', str(q))
