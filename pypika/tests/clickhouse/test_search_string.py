import unittest

from parameterized import parameterized

from pypika import Field
from pypika.clickhouse.search_string import (
    Match,
    Like,
    NotLike,
    MultiSearchAny,
    MultiMatchAny,
)


class TestSearchString(unittest.TestCase):
    @parameterized.expand(
        [
            (Match(Field("name"), "twheys"), "match(toString(\"name\"),'twheys')"),
            (Like(Field("name"), "twheys"), "like(toString(\"name\"),'twheys')"),
            (NotLike(Field("name"), "twheys"), "notLike(toString(\"name\"),'twheys')"),
        ]
    )
    def test_search_string(self, func, expected):
        self.assertEqual(func, expected)


class TestMultiSearch(unittest.TestCase):
    @parameterized.expand(
        [
            (
                MultiSearchAny(Field("name"), ["sarah", "connor"]),
                "multiSearchAny(toString(\"name\"),['sarah','connor'])",
            ),
            (
                MultiMatchAny(Field("name"), ["sarah", "connor"]),
                "multiMatchAny(toString(\"name\"),['sarah','connor'])",
            ),
        ]
    )
    def test_multi_search_string(self, func, expected):
        self.assertEqual(func.get_sql(), expected)
