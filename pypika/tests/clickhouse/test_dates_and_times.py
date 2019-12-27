import unittest

from parameterized import parameterized

from pypika import Field
from pypika.clickhouse.dates_and_times import (
    AddDays,
    AddHours,
    AddMinutes,
    AddMonths,
    AddQuarters,
    AddSeconds,
    AddWeeks,
    AddYears,
    FormatDateTime,
    SubtractDays,
    SubtractHours,
    SubtractMinutes,
    SubtractMonths,
    SubtractQuarters,
    SubtractSeconds,
    SubtractWeeks,
    SubtractYears,
    ToYYYYMM,
)


class TestFormatDateTime(unittest.TestCase):
    @parameterized.expand(
        [
            (FormatDateTime(Field("created"), "%V"), "formatDateTime(created,'%V')"),
            (
                FormatDateTime(Field("created"), "%Y-%m-%d"),
                "formatDateTime(created,'%Y-%m-%d')",
            ),
        ]
    )
    def test_get_sql(self, func, expected):
        self.assertEqual(func.get_sql(), expected)


class TestAddSubtractDt(unittest.TestCase):
    @parameterized.expand(
        [
            (AddYears(Field("created"), 1), "addYears(created,1)"),
            (AddMonths(Field("created"), 2), "addMonths(created,2)"),
            (AddWeeks(Field("created"), 3), "addWeeks(created,3)"),
            (AddDays(Field("created"), 4), "addDays(created,4)"),
            (AddHours(Field("created"), 5), "addHours(created,5)"),
            (AddMinutes(Field("created"), 6), "addMinutes(created,6)"),
            (AddSeconds(Field("created"), 7), "addSeconds(created,7)"),
            (AddQuarters(Field("created"), 8), "addQuarters(created,8)"),
            (SubtractYears(Field("created"), 9), "subtractYears(created,9)"),
            (SubtractMonths(Field("created"), 10), "subtractMonths(created,10)"),
            (SubtractWeeks(Field("created"), 11), "subtractWeeks(created,11)"),
            (SubtractDays(Field("created"), 12), "subtractDays(created,12)"),
            (SubtractHours(Field("created"), 13), "subtractHours(created,13)"),
            (SubtractHours(Field("created"), 13), "subtractHours(created,13)"),
            (SubtractMinutes(Field("created"), 14), "subtractMinutes(created,14)"),
            (SubtractSeconds(Field("created"), 15), "subtractSeconds(created,15)"),
            (SubtractQuarters(Field("created"), 16), "subtractQuarters(created,16)"),
        ]
    )
    def test_get_sql(self, func, expected):
        self.assertEqual(func.get_sql(), expected)


class TestToYYYYMM(unittest.TestCase):
    def test_get_sql(self):
        self.assertEqual(ToYYYYMM(Field("created").get_sql()), "toYYYYMM(created)")
