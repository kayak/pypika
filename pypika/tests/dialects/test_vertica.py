import unittest

from pypika import Table
from pypika.dialects import VerticaQuery


class CopyCSVTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_copy_from_file(self):
        q1 = VerticaQuery \
            .from_file('/path/to/file') \
            .copy_('abc')

        q2 = VerticaQuery \
            .from_file('/path/to/file') \
            .copy_(self.table_abc)

        self.assertEqual('COPY "abc" FROM "/path/to/file" PARSER fcsvparser()', str(q1))
        self.assertEqual('COPY "abc" FROM "/path/to/file" PARSER fcsvparser()', str(q2))
