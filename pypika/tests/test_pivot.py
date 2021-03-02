import unittest

from pypika import (
    Query,
    Table,
)
from pypika.pivot import pivot
import pypika.functions as fn


class PivotTests(unittest.TestCase):
    table_test = Table("monthly_sales")

    def test_pivot(self):
        q = (
            Query.from_(self.table_test)
            .groupby(self.table_test.id)
            .select(self.table_test.id, *pivot(self.table_test.month, self.table_test.sales, ["JAN", "FEB"], fn.Sum))
        )

        self.assertEqual(
            '''
            SELECT "id",SUM(CASE WHEN "month"=\'JAN\' THEN "sales" ELSE NULL END) "JAN",
            SUM(CASE WHEN "month"=\'FEB\' THEN "sales" ELSE NULL END) "FEB" FROM "monthly_sales" GROUP BY "id"
            ''',
            str(q),
        )
