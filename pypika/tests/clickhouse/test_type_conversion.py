import unittest

from pypika import Field
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
    def test_basic_types_field(self):
        types_map = (
            ('toString("field_name")', ToString,),
            ('toInt8("field_name")', ToInt8,),
            ('toInt16("field_name")', ToInt16,),
            ('toInt32("field_name")', ToInt32,),
            ('toInt64("field_name")', ToInt64,),
            ('toUInt8("field_name")', ToUInt8,),
            ('toUInt16("field_name")', ToUInt16,),
            ('toUInt32("field_name")', ToUInt32,),
            ('toUInt64("field_name")', ToUInt64,),
            ('toFloat32("field_name")', ToFloat32,),
            ('toFloat64("field_name")', ToFloat64,),
            ('toFloat64("field_name")', ToFloat64,),
            ('toDate("field_name")', ToDate,),
            ('toDateTime("field_name")', ToDateTime,),
        )

        for expected, cls in types_map:
            self.assertEqual(
                cls(Field('field_name')),
                expected
            )

    def test_basic_types_value(self):
        types_map = (
            ("toString('100')", ToString,),
            ("toInt8('100')", ToInt8,),
            ("toInt16('100')", ToInt16,),
            ("toInt32('100')", ToInt32,),
            ("toInt64('100')", ToInt64,),
            ("toUInt8('100')", ToUInt8,),
            ("toUInt16('100')", ToUInt16,),
            ("toUInt32('100')", ToUInt32,),
            ("toUInt64('100')", ToUInt64,),
            ("toFloat32('100')", ToFloat32,),
            ("toFloat64('100')", ToFloat64,),
            ("toFloat64('100')", ToFloat64,),
            ("toDate('100')", ToDate,),
            ("toDateTime('100')", ToDateTime,),
        )

        for expected, cls in types_map:
            self.assertEqual(cls('100'), expected)


class TestToFixedString(unittest.TestCase):

    def test_to_fixed_string_field(self):
        self.assertEqual(
            ToFixedString(Field('field_name'), 100).get_sql(),
            'toFixedString("field_name",100)'
        )

    def test_to_fixed_string_value(self):
        self.assertEqual(
            ToFixedString('100', 100).get_sql(),
            "toFixedString('100',100)"
        )
