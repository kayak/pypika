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
    types_map = (
        ('toString("field_name")', ToString, ),
        ('toInt8("field_name")', ToInt8, ),
        ('toInt16("field_name")', ToInt16, ),
        ('toInt32("field_name")', ToInt32, ),
        ('toInt64("field_name")', ToInt64, ),
        ('toUInt8("field_name")', ToUInt8, ),
        ('toUInt16("field_name")', ToUInt16, ),
        ('toUInt32("field_name")', ToUInt32, ),
        ('toUInt64("field_name")', ToUInt64, ),
        ('toFloat32("field_name")', ToFloat32, ),
        ('toFloat64("field_name")', ToFloat64, ),
        ('toFloat64("field_name")', ToFloat64, ),
        ('toDate("field_name")', ToDate, ),
        ('toDateTime("field_name")', ToDateTime, ),
    )

    def test_basic_types(self):
        for expected, cls in self.types_map:
            self.assertEqual(
                cls(Field('field_name')),
                expected
            )


class TestToFixedString(unittest.TestCase):

    def test_to_fixed_string(self):
        self.assertEqual(
            ToFixedString(Field('field_name'), 100).get_sql(),
            'toFixedString("field_name",100)'
        )
