import unittest

from pypika import AliasedQuery, Table
from pypika.dialects import PostgreSQLQuery


class ConflictNamespaceTests(unittest.TestCase):
    def test_cte_conflict_columns_are_unqualified(self):
        for do_nothing in (False, True):
            with self.subTest(do_nothing=do_nothing):
                target = Table('target')
                source = Table('source')
                inserted = AliasedQuery('inserted')
                insert = (
                    PostgreSQLQuery.into(target)
                    .columns('id', 'value')
                    .from_(source)
                    .select(source.id, source.value)
                    .on_conflict(target.id)
                )
                insert = insert.do_nothing() if do_nothing else insert.do_update(target.value)
                insert = insert.returning(target.id)
                query = (
                    PostgreSQLQuery.with_(insert, 'inserted')
                    .from_(inserted)
                    .join(source)
                    .on(inserted.id == source.id)
                    .select(inserted.id)
                )
                action = 'DO NOTHING' if do_nothing else 'DO UPDATE SET "value"=EXCLUDED."value"'
                expected = (
                    'WITH inserted AS (INSERT INTO "target" ("id","value") '
                    'SELECT "id","value" FROM "source" ON CONFLICT ("id") '
                    + action
                    + ' RETURNING "id") SELECT "inserted"."id" FROM inserted '
                    'JOIN "source" ON "inserted"."id"="source"."id"'
                )
                self.assertEqual(expected, str(query))

    def test_aliased_insert_table_does_not_qualify_conflict_columns(self):
        target = Table('target').as_('t')
        field = target.id
        query = PostgreSQLQuery.into(target).insert(1, 2).on_conflict(field).do_update(target.value)
        self.assertEqual(
            'INSERT INTO "target" AS "t" VALUES (1,2) ON CONFLICT ("id") ' 'DO UPDATE SET "value"=EXCLUDED."value"',
            query.get_sql(as_keyword=True),
        )
        self.assertEqual('"t"."id"', field.get_sql(quote_char='"'))

    def test_update_value_keeps_table_namespace(self):
        target = Table('target').as_('t')
        query = (
            PostgreSQLQuery.into(target).insert(1, 2).on_conflict(target.id).do_update(target.value, target.value + 1)
        )
        self.assertEqual(
            'INSERT INTO "target" AS "t" VALUES (1,2) ON CONFLICT ("id") ' 'DO UPDATE SET "value"="t"."value"+1',
            query.get_sql(as_keyword=True),
        )
