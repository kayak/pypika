import unittest

from pypika import (
    Table,
    Columns,
    Query,
)

from pypika.utils import sqlescape

class InjectTableTests(unittest.TestCase):
    table = Table('the_table')
    def test_anti_inject_query(self):
        with self.subTest("with  keyword"):
            unsafe = "John\'; DROP TABLE the_table --"
            q = Query.from_(self.table).select('*').where(self.table.name == unsafe)
            print(str(q))
            self.assertEqual("""SELECT * FROM "the_table" WHERE "name"='John\\''; DROP TABLE the_table --'""", str(q))

        with self.subTest("sqlescape test"):
            self.assertEqual(sqlescape(12),12)
            self.assertEqual(sqlescape("test'dds"),"test\\'dds")

