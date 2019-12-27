import unittest

from parameterized import parameterized

from pypika import ClickHouseQuery
from pypika import Field
from pypika import Table
from pypika.clickhouse.type_conversion import (
    ToString,
    ToInt8,
    ToInt16,
    ToInt32,
    ToInt64,
    ToUInt8,
    ToUInt16,
    ToUInt32,
    ToUInt64,
    ToFloat32,
    ToFloat64,
    ToDate,
    ToDateTime,
    ToFixedString,
)


class TestBasicTypeConverters(unittest.TestCase):
    @parameterized.expand(
        [
            ('toString("field_name")', ToString(Field("field_name")),),
            ('toInt8("field_name")', ToInt8(Field("field_name")),),
            ('toInt16("field_name")', ToInt16(Field("field_name")),),
            ('toInt32("field_name")', ToInt32(Field("field_name")),),
            ('toInt64("field_name")', ToInt64(Field("field_name")),),
            ('toUInt8("field_name")', ToUInt8(Field("field_name")),),
            ('toUInt16("field_name")', ToUInt16(Field("field_name")),),
            ('toUInt32("field_name")', ToUInt32(Field("field_name")),),
            ('toUInt64("field_name")', ToUInt64(Field("field_name")),),
            ('toFloat32("field_name")', ToFloat32(Field("field_name")),),
            ('toFloat64("field_name")', ToFloat64(Field("field_name")),),
            ('toFloat64("field_name")', ToFloat64(Field("field_name")),),
            ('toDate("field_name")', ToDate(Field("field_name")),),
            ('toDateTime("field_name")', ToDateTime(Field("field_name")),),
            (
                'toFixedString("field_name",100)',
                ToFixedString(Field("field_name"), 100),
            ),
        ]
    )
    def test_basic_types_field(self, expected, func):
        self.assertEqual(func, expected)

    @parameterized.expand(
        [
            ("toString('100')", ToString("100"),),
            ("toInt8('100')", ToInt8("100"),),
            ("toInt16('100')", ToInt16("100"),),
            ("toInt32('100')", ToInt32("100"),),
            ("toInt64('100')", ToInt64("100"),),
            ("toUInt8('100')", ToUInt8("100"),),
            ("toUInt16('100')", ToUInt16("100"),),
            ("toUInt32('100')", ToUInt32("100"),),
            ("toUInt64('100')", ToUInt64("100"),),
            ("toFloat32('100')", ToFloat32("100"),),
            ("toFloat64('100')", ToFloat64("100"),),
            ("toFloat64('100')", ToFloat64("100"),),
            ("toDate('100')", ToDate("100"),),
            ("toDateTime('100')", ToDateTime("100"),),
            ("toFixedString('100',100)", ToFixedString("100", 100),),
        ]
    )
    def test_basic_types_value(self, expected, func):
        self.assertEqual(func, expected)


class TestToFixedString(unittest.TestCase):
    def test_get_sql_with_table(self):
        table = Table("example")
        query = (
            ClickHouseQuery.from_(table)
            .select(table.name,)
            .where(table.name == ToFixedString("name", 50),)
        )

        self.assertEqual(
            'SELECT "name" FROM "example" WHERE "name"=toFixedString(\'name\',50)',
            query.get_sql(),
        )
