import unittest

from pypika import Field
from pypika.clickhouse.search_string import Match, Like, NotLike, MultiSearchAny, MultiMatchAny


class TestSearchString(unittest.TestCase):
    def test_search_string(self):
        search_types = (
            (Match, "match(toString(\"name\"),'twheys')"),
            (Like, "like(toString(\"name\"),'twheys')"),
            (NotLike, "notLike(toString(\"name\"),'twheys')"),
        )

        for cls, expected in search_types:
            self.assertEqual(
                cls(Field('name'), 'twheys'),
                expected
            )


class TestMultiSearch(unittest.TestCase):
    def test_multi_search_string(self):
        search_types = (
            (MultiSearchAny, "multiSearchAny(toString(\"name\"),['sarah','connor'])"),
            (MultiMatchAny, "multiMatchAny(toString(\"name\"),['sarah','connor'])"),
        )

        for cls, expected in search_types:
            self.assertEqual(
                cls(Field('name'), ['sarah', 'connor']).get_sql(),
                expected
            )
