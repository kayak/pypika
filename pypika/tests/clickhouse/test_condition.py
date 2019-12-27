import unittest

from parameterized import parameterized

from pypika.clickhouse.condition import MultiIf
from pypika.clickhouse.type_conversion import ToFixedString

from pypika import Field
from pypika.clickhouse.condition import If


class TestIfCondition(unittest.TestCase):
    @parameterized.expand(
        [
            (
                If(
                    Field("filmmaker").isnull(),
                    ToFixedString("Tarantino", 20),
                    Field("filmmaker"),
                ),
                "if(filmmaker IS NULL,toFixedString('Tarantino',20),filmmaker)",
            ),
            (
                If(
                    Field("created") == Field("updated"),
                    ToFixedString("yes", 3),
                    ToFixedString("no", 3),
                ),
                "if(created=updated,toFixedString('yes',3),toFixedString('no',3))",
            ),
        ]
    )
    def test_get_sql(self, func, expected):
        self.assertEqual(func.get_sql(), expected)


class TestMultiIfCondition(unittest.TestCase):
    @parameterized.expand(
        [
            (
                MultiIf(
                    Field("filmmaker").isnull(),
                    ToFixedString("Tarantino", 20),
                    Field("filmmaker") == ToFixedString("undefined", 20),
                    ToFixedString("Tarantino", 20),
                    Field("filmmaker"),
                ),
                "multiIf(filmmaker IS NULL,"
                "toFixedString('Tarantino',20),"
                "filmmaker=toFixedString('undefined',20),"
                "toFixedString('Tarantino',20),filmmaker)",
            ),
            (
                MultiIf(
                    Field("color") == ToFixedString("black", 20),
                    ToFixedString("dark", 20),
                    Field("color") == ToFixedString("grey", 20),
                    ToFixedString("dark", 20),
                    Field("color") == ToFixedString("white", 20),
                    ToFixedString("light", 20),
                    ToFixedString("undefined", 20),
                ),
                "multiIf(color=toFixedString('black',20),"
                "toFixedString('dark',20),"
                "color=toFixedString('grey',20),"
                "toFixedString('dark',20),"
                "color=toFixedString('white',20),"
                "toFixedString('light',20),"
                "toFixedString('undefined',20))",
            ),
        ]
    )
    def test_get_sql(self, func, expected):
        self.assertEqual(func.get_sql(), expected)
