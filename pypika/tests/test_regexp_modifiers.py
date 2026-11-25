import unittest

from pypika import Field, functions
from pypika.terms import NullValue


class RegexpModifiersTests(unittest.TestCase):
    def test_omitted_modifiers(self):
        for func in (functions.RegexpLike, functions.RegexpMatches):
            with self.subTest(func=func):
                name = func(Field("value"), "pattern").name
                expected = name + "(\"value\",'pattern')"
                self.assertEqual(expected, str(func(Field("value"), "pattern")))
                self.assertEqual(expected, str(func(Field("value"), "pattern", None)))

    def test_explicit_modifiers(self):
        for func in (functions.RegexpLike, functions.RegexpMatches):
            for modifiers, sql in [("", "''"), ("i", "'i'"), (NullValue(), "null")]:
                with self.subTest(func=func, modifiers=modifiers):
                    expression = func(Field("value"), "pattern", modifiers)
                    expected = expression.name + "(\"value\",'pattern'," + sql + ")"
                    self.assertEqual(expected, str(expression))
