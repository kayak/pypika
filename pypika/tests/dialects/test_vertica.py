import unittest

from pypika import Table
from pypika.dialects import VerticaQuery


class CopyCSVTests(unittest.TestCase):
    table_abc = Table('abc')

    def test_copy_csv_from_file(self):
        fp = '/path/to/file'
        q = VerticaQuery \
            .from_file(fp) \
            .copy_(self.table_abc)

        self.assertEqual('COPY "abc" FROM "/path/to/file" PARSER fcsvparser()', str(q))
