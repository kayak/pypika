import unittest

from pypika import (
    OracleQuery,
    Table,
)


class ROWNUMTests(unittest.TestCase):

    def test_normal_select(self):
        stuff = Table('stuff')
        q = OracleQuery.from_(stuff).select('*').where(OracleQuery.RowNum <= 5)

        self.assertEqual('SELECT * FROM "stuff" WHERE ROWNUM<=5', str(q))
