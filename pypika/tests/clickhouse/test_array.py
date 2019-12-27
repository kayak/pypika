import unittest

from parameterized import parameterized

from pypika import Field
from pypika.clickhouse.array import HasAny, Array, Length, Empty, NotEmpty
from pypika.clickhouse.type_conversion import ToInt64, ToFixedString


class TestArray(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "['ridley', 'scott', 'jimi', 'hendrix']",
                Array(["ridley", "scott", "jimi", "hendrix"]),
            ),
            ("[1, 2, 3, 4]", Array([1, 2, 3, 4]),),
            (
                "[toInt64('1'),toInt64('2'),toInt64('3'),toInt64('4')]",
                Array(["1", "2", "3", "4"], ToInt64),
            ),
            (
                "[toFixedString('mogwai',10),toFixedString('mono',10),toFixedString('bonobo',10)]",
                Array(["mogwai", "mono", "bonobo"], ToFixedString, {"length": 10}),
            ),
        ]
    )
    def test_get_sql(self, expected: str, array: Array):
        self.assertEqual(expected, array.get_sql())


class TestHasAny(unittest.TestCase):
    @parameterized.expand(
        [
            (
                'hasAny("mental_abilities","physical_abilities")',
                HasAny(Field("mental_abilities"), Field("physical_abilities")),
            ),
            ("hasAny([1, 2, 3, 4],[3])", HasAny(Array([1, 2, 3, 4]), Array([3]))),
            (
                "hasAny(\"bands\",[toFixedString('port-royal',20),toFixedString('hammock',20)])",
                HasAny(
                    Field("bands"),
                    Array(["port-royal", "hammock"], ToFixedString, {"length": 20}),
                ),
            ),
        ]
    )
    def test_get_sql(self, expected: str, func: HasAny):
        self.assertEqual(expected, func.get_sql())


class TestLength(unittest.TestCase):
    @parameterized.expand(
        [
            ('length("tags")', Length(Field("tags")),),
            ("length([1, 2, 3])", Length(Array([1, 2, 3]))),
        ]
    )
    def test_get_sql(self, expected: str, func: Length):
        self.assertEqual(expected, func.get_sql())


class TestEmpty(unittest.TestCase):
    @parameterized.expand(
        [
            ('empty("tags")', Empty(Field("tags")),),
            ("empty([1, 2, 3])", Empty(Array([1, 2, 3]))),
        ]
    )
    def test_get_sql(self, expected: str, func: Empty):
        self.assertEqual(expected, func.get_sql())


class TestNotEmpty(unittest.TestCase):
    @parameterized.expand(
        [
            ('notEmpty("tags")', NotEmpty(Field("tags")),),
            ("notEmpty([1, 2, 3])", NotEmpty(Array([1, 2, 3]))),
        ]
    )
    def test_get_sql(self, expected: str, func: NotEmpty):
        self.assertEqual(expected, func.get_sql())
