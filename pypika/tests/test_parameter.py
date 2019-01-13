import unittest

from pypika import (
    Tables,
    Query,
    Parameter
)


class ParametrizedTests(unittest.TestCase):
    table_abc, table_efg = Tables('abc', 'efg')

    def test_param_insert(self):
        q = Query.into(self.table_abc).columns('a', 'b', 'c').insert(Parameter('?'), Parameter('?'), Parameter('?'))

        self.assertEqual('INSERT INTO "abc" ("a","b","c") VALUES (?,?,?)', q.get_sql())

    def test_param_select_join(self):
        q = Query.from_(self.table_abc).select('*').where(self.table_abc.category == Parameter('?'))\
            .join(self.table_efg).on(self.table_abc.id == self.table_efg.abc_id)\
            .where(self.table_efg.date >= Parameter('?')).limit(10)

        self.assertEqual(
            'SELECT * FROM "abc" JOIN "efg" ON "abc"."id"="efg"."abc_id" WHERE "abc"."category"=? AND "efg"."date">=? LIMIT 10',
            q.get_sql())

    def test_param_select_subquery(self):
        q = Query.from_(self.table_abc).select('*').where(self.table_abc.category == Parameter('?'))\
            .where(self.table_abc.id.isin(
                Query.from_(self.table_efg).select(self.table_efg.abc_id).where(self.table_efg.date >= Parameter('?'))
            )).limit(10)

        self.assertEqual(
            'SELECT * FROM "abc" WHERE "category"=? AND "id" IN (SELECT "abc_id" FROM "efg" WHERE "date">=?) LIMIT 10',
            q.get_sql())

    def test_join(self):
        subquery = Query.from_(self.table_efg).select(self.table_efg.fiz, self.table_efg.buz).where(self.table_efg.buz == Parameter('?'))

        q = Query.from_(self.table_abc).join(subquery).on(
            self.table_abc.bar == subquery.buz
        ).select(self.table_abc.foo, subquery.fiz).where(self.table_abc.bar == Parameter('?'))

        self.assertEqual(
            'SELECT "abc"."foo","sq0"."fiz" FROM "abc" JOIN (SELECT "fiz","buz" FROM "efg" WHERE "buz"=?)'
            ' "sq0" ON "abc"."bar"="sq0"."buz" WHERE "abc"."bar"=?',
            q.get_sql())
