import unittest

from parameterized import parameterized

from pypika import Field
from pypika.clickhouse.dates_and_times import FormatDateTime


class TestFormatDateTime(unittest.TestCase):

    @parameterized.expand([
        (FormatDateTime(Field('created'), '%V'), "formatDateTime(created,'%V')"),
        (FormatDateTime(Field('created'), '%Y-%m-%d'), "formatDateTime(created,'%Y-%m-%d')"),
    ])
    def test_get_sql(self, func, expected):
        self.assertEqual(func.get_sql(), expected)
