import unittest

from pypika.terms import ValueWrapper


class StringTests(unittest.TestCase):
    def test_inline_string_concatentation(self):
        self.assertEqual("'it''s'", ValueWrapper("it's").get_sql())
