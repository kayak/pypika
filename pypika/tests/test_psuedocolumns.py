import unittest

from pypika.pseudocolumns import (
    ColumnValue,
    ObjectID,
    ObjectValue,
    RowID,
    RowNum,
    SysDate,
)


class PseudocolumnsTest(unittest.TestCase):
    def test_column_value(self):
        self.assertEqual('COLUMN_VALUE', ColumnValue)

    def test_object_id(self):
        self.assertEqual('OBJECT_ID', ObjectID)

    def test_object_value(self):
        self.assertEqual('OBJECT_VALUE', ObjectValue)

    def test_row_id(self):
        self.assertEqual('ROWID', RowID)

    def test_row_num(self):
        self.assertEqual('ROWNUM', RowNum)

    def test_sys_date(self):
        self.assertEqual('SYSDATE', SysDate)
