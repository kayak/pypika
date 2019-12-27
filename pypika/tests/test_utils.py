import unittest

from pypika import utils

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


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
