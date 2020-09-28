import unittest

from parameterized import parameterized

from pypika import Field
from pypika.clickhouse.nullable_arg import IfNull
from pypika.clickhouse.type_conversion import ToFixedString


class TestSearchString(unittest.TestCase):
    @parameterized.expand(
        [
            (
                IfNull(Field("name"), Field("login")),
                "ifNull(name,login)",
            ),
            (
                IfNull(Field("builder"), ToFixedString("pypika", 100)),
                "ifNull(builder,toFixedString('pypika',100))",
            ),
        ]
    )
    def test_get_sql(self, func, expected):
        self.assertEqual(func.get_sql(), expected)
