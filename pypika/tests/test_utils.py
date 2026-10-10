import unittest

from pypika import utils
from pypika.enums import Dialects
from pypika.utils import escape_backslashes

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class EscapeBackslashesTests(unittest.TestCase):
    def test_backslashes_doubled_for_mysql(self):
        self.assertEqual("a\\\\b", escape_backslashes("a\\b", Dialects.MYSQL))

    def test_backslashes_doubled_for_clickhouse(self):
        self.assertEqual("a\\\\b", escape_backslashes("a\\b", Dialects.CLICKHOUSE))

    def test_trailing_backslash_doubled_for_mysql(self):
        self.assertEqual("bar\\\\", escape_backslashes("bar\\", Dialects.MYSQL))

    def test_multiple_consecutive_backslashes_all_doubled(self):
        self.assertEqual("a\\\\\\\\b", escape_backslashes("a\\\\b", Dialects.MYSQL))

    def test_backslash_and_quote_only_backslash_doubled(self):
        self.assertEqual("it\\\\'s", escape_backslashes("it\\'s", Dialects.MYSQL))

    def test_literal_backslash_for_other_dialects(self):
        for dialect in (Dialects.POSTGRESQL, Dialects.SQLLITE, Dialects.MSSQL, Dialects.ORACLE, Dialects.SNOWFLAKE):
            self.assertEqual("a\\b", escape_backslashes("a\\b", dialect), dialect)

    def test_literal_backslash_without_dialect(self):
        self.assertEqual("a\\b", escape_backslashes("a\\b", None))


class ImmutabilityTests(unittest.TestCase):
    def test_raise_attribute_error_for_deepcopy(self):
        with self.assertRaises(AttributeError):
            utils.raise_attribute_for_copy("Test", "__deepcopy__")

    def test_raise_attribute_error_for_getstate(self):
        with self.assertRaises(AttributeError):
            utils.raise_attribute_for_copy("Test", "__getstate__")

    def test_raise_attribute_error_for_setstate(self):
        with self.assertRaises(AttributeError):
            utils.raise_attribute_for_copy("Test", "__setstate__")

    def test_raise_attribute_error_for_getnewargs(self):
        with self.assertRaises(AttributeError):
            utils.raise_attribute_for_copy("Test", "__getnewargs__")
