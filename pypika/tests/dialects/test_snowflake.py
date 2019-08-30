import unittest

from pypika import Table
from pypika.dialects import SnowflakeQuery


class QuoteTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_no_double_quotes(self):
        q = SnowflakeQuery.from_('abc').select('a')

        self.assertEqual('SELECT a FROM abc', str(q))
