import unittest

from pypika import Table, Query, QueryException
from pypika.dialects import PostgreSQLQuery
from pypika.terms import JSONField


class InsertTests(unittest.TestCase):
    table_abc = Table('abc')
    json_field = JSONField('json')

    def test_array_keyword(self):
        q = PostgreSQLQuery.into(self.table_abc).insert(1, [1, "a", True])

        self.assertEqual('INSERT INTO "abc" VALUES (1,ARRAY[1,\'a\',true])', str(q))

    def test_json_contains(self):
        q = PostgreSQLQuery.from_(
            self.table_abc,
        ).select("*").where(
            self.json_field.contains(
                {"dates": "2018-07-10 - 2018-07-17"},
            ),
        )

        self.assertEqual(
            'SELECT * '
            'FROM "abc" '
            'WHERE "json"@>\'{"dates": "2018-07-10 - 2018-07-17"}\'',
            str(q),
        )

        q = PostgreSQLQuery.from_(
            self.table_abc
        ).select("*").where(
            self.json_field.contains(
                '{"dates": "2018-07-10 - 2018-07-17"}'
            )
        )
        self.assertEqual(
            'SELECT * '
            'FROM "abc" '
            'WHERE "json"@>\'{"dates": "2018-07-10 - 2018-07-17"}\'',
            str(q),
        )

    def test_json_has_key(self):
        q = PostgreSQLQuery.from_(self.table_abc).select("*").where(
            self.json_field.has_key("dates"),
        )

        self.assertEqual(
            'SELECT * FROM "abc" WHERE "json"?\'dates\'', str(q),
        )

        q = PostgreSQLQuery.from_(self.table_abc).select("*").where(
            self.json_field.has_key(JSONField("dates")),
        )

        self.assertEqual(
            'SELECT * FROM "abc" WHERE "json"?\'dates\'', str(q),
        )

    def test_json_contained_by(self):
        q = PostgreSQLQuery.from_(self.table_abc).select('*').where(
            self.json_field.contained_by(
                '{"dates": "2018-07-10 - 2018-07-17", "imported": "8"}',
            ),
        )

        self.assertEqual(
            'SELECT * FROM "abc" WHERE "json"<@\'{"dates": "2018-07-10 - 2018-07-17", "imported": "8"}\'',
            str(q),
        )

        q = PostgreSQLQuery.from_(self.table_abc).select('*').where(
            self.json_field.contained_by(
                ["One", 'Two', "Three"],
            ),
        )

        self.assertEqual(
            'SELECT * FROM "abc" WHERE "json"<@\'["One", "Two", "Three"]\'', str(q),
        )

        q = PostgreSQLQuery.from_(self.table_abc).select('*').where(
            self.json_field.contained_by('["One", "Two", "Three"]') & self.table_abc.id == 26
        )

        self.assertEqual(
            'SELECT * FROM "abc" WHERE "json"<@\'["One", "Two", "Three"]\' AND "id"=26', str(q),
        )

    def test_json_has_keys(self):
        q = PostgreSQLQuery.from_(self.table_abc).select("*").where(
            self.json_field.has_keys(['dates', 'imported']),
        )

        self.assertEqual(
            'SELECT * FROM "abc" WHERE "json"?&ARRAY[\'dates\',\'imported\']', str(q)
        )

    def test_json_has_any_keys(self):
        q = PostgreSQLQuery.from_(self.table_abc).select("*").where(
            self.json_field.has_any_keys(['dates', 'imported']),
        )

        self.assertEqual(
            'SELECT * FROM "abc" WHERE "json"?|ARRAY[\'dates\',\'imported\']', str(q)
        )

    def test_get_value_by_key(self):
        q = PostgreSQLQuery.from_(self.table_abc).select(self.json_field.get_value_by_key(['dates', 'import']))

        self.assertEqual(
            'SELECT "json"->\'dates\'->>\'import\' FROM "abc"', str(q)
        )

        q = PostgreSQLQuery.from_(self.table_abc).select(self.json_field.get_value_by_key('dates'))

        self.assertEqual(
            'SELECT "json"->\'dates\' FROM "abc"', str(q)
        )

    def test_not_valid_values(self):
        q = Query.from_(self.table_abc).select("*").where(self.json_field.has_key("dates"))
        self.assertRaises(QueryException, str, q)

        self.assertRaises(QueryException, self.json_field.has_keys, 'dates')
        self.assertRaises(QueryException, self.json_field.has_any_keys, 'dates')
        self.assertRaises(QueryException, self.json_field.get_value_by_key, {'test':'test'})
        self.assertRaises(QueryException, self.json_field.has_key, 1)

