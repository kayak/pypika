import unittest

from pypika import utils
from pypika.utils import format_quotes

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class FormatQuotesTests(unittest.TestCase):
    def test_backslashes_are_escaped(self):
        self.assertEqual("'a\\\\b'", format_quotes("a\\b", "'"))

    def test_trailing_backslash_keeps_closing_quote_intact(self):
        self.assertEqual("'bar\\\\'", format_quotes("bar\\", "'"))

    def test_backslash_and_quote_char_are_both_escaped(self):
        self.assertEqual("'it\\\\''s'", format_quotes("it\\'s", "'"))

    def test_custom_quote_char_with_backslash_and_quote(self):
        self.assertEqual("`it\\\\``s`", format_quotes("it\\`s", "`"))

    def test_no_quote_char_leaves_value_untouched(self):
        self.assertEqual("a\\b", format_quotes("a\\b", None))
        self.assertEqual("a\\b", format_quotes("a\\b", ""))


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
