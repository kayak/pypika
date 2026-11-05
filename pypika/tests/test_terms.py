from unittest import TestCase

from pypika import JSON, Field, Query, Table
from pypika.dialects import ClickHouseQuery, MSSQLQuery, MySQLQuery, PostgreSQLQuery, SQLLiteQuery
from pypika.terms import AtTimezone


class FieldAliasTests(TestCase):
    t = Table("test", alias="crit")

    def test_when_alias_specified(self):
        c1 = Field("foo", alias="bar")
        self.assertEqual('bar', str(c1.alias))

        c1 = Field("foo").as_("bar")
        self.assertEqual('bar', str(c1.alias))


class FieldInitTests(TestCase):
    def test_init_with_str_table(self):
        test_table_name = "test_table"
        field = Field(name="name", table=test_table_name)
        self.assertEqual(field.table, Table(name=test_table_name))


class FieldHashingTests(TestCase):
    def test_tabled_eq_fields_equally_hashed(self):
        client_name1 = Field(name="name", table=Table("clients"))
        client_name2 = Field(name="name", table=Table("clients"))
        self.assertTrue(hash(client_name1) == hash(client_name2))

    def test_tabled_ne_fields_differently_hashed(self):
        customer_name = Field(name="name", table=Table("customers"))
        client_name = Field(name="name", table=Table("clients"))
        self.assertTrue(hash(customer_name) != hash(client_name))

    def test_non_tabled_aliased_eq_fields_equally_hashed(self):
        self.assertTrue(hash(Field(name="A", alias="my_a")) == hash(Field(name="A", alias="my_a")))

    def test_non_tabled_aliased_ne_fields_differently_hashed(self):
        self.assertTrue(hash(Field(name="A", alias="my_a1")) != hash(Field(name="A", alias="my_a2")))

    def test_non_tabled_eq_fields_equally_hashed(self):
        self.assertTrue(hash(Field(name="A")) == hash(Field(name="A")))

    def test_non_tabled_ne_fields_differently_hashed(self):
        self.assertTrue(hash(Field(name="A")) != hash(Field(name="B")))


class AtTimezoneTests(TestCase):
    def test_when_interval_not_specified(self):
        query = Query.from_("customers").select(AtTimezone("date", "US/Eastern"))
        self.assertEqual('SELECT "date" AT TIME ZONE \'US/Eastern\' FROM "customers"', str(query))

    def test_when_interval_specified(self):
        query = Query.from_("customers").select(AtTimezone("date", "-06:00", interval=True))
        self.assertEqual(
            'SELECT "date" AT TIME ZONE INTERVAL \'-06:00\' FROM "customers"',
            str(query),
        )

    def test_when_alias_specified(self):
        query = Query.from_("customers").select(AtTimezone("date", "US/Eastern", alias="alias1"))
        self.assertEqual(
            'SELECT "date" AT TIME ZONE \'US/Eastern\' "alias1" FROM "customers"',
            str(query),
        )

    def test_passes_kwargs_to_field_get_sql(self):
        customers = Table("customers")
        accounts = Table("accounts")
        query = (
            Query.from_(customers)
            .join(accounts)
            .on(customers.account_id == accounts.account_id)
            .select(AtTimezone(customers.date, "US/Eastern", alias="alias1"))
        )

        self.assertEqual(
            'SELECT "customers"."date" AT TIME ZONE \'US/Eastern\' "alias1" '
            'FROM "customers" JOIN "accounts" ON "customers"."account_id"="accounts"."account_id"',
            query.get_sql(with_namespace=True),
        )


class IdentifierEscapingTests(TestCase):
    def test_escape_identifier_quotes(self):
        customers = Table('customers"')
        customer_id = getattr(customers, '"id')
        email = getattr(customers, 'email"').as_('customer_email"')

        query = (
            Query.from_(customers)
            .select(customer_id, email)
            .where(customer_id == "abc")
            .where(email == "abc@abc.com")
            .orderby(email, customer_id)
        )

        self.assertEqual(
            'SELECT """id","email""" "customer_email""" '
            'FROM "customers""" WHERE """id"=\'abc\' AND "email"""=\'abc@abc.com\' '
            'ORDER BY "customer_email""","""id"',
            query.get_sql(),
        )

    def test_backslashes_in_identifiers_are_never_escaped(self):
        # Delimited identifiers treat a backslash as a literal character on every
        # backend, so they must render identically regardless of dialect
        table = Table("a\\b")
        field = Field("f\\g", table=table)
        expected = {
            Query: 'SELECT "f\\g" FROM "a\\b"',
            MySQLQuery: "SELECT `f\\g` FROM `a\\b`",
            PostgreSQLQuery: 'SELECT "f\\g" FROM "a\\b"',
            SQLLiteQuery: 'SELECT "f\\g" FROM "a\\b"',
            MSSQLQuery: 'SELECT "f\\g" FROM "a\\b"',
            ClickHouseQuery: 'SELECT "f\\g" FROM "a\\b"',
        }
        for query_cls, expected_sql in expected.items():
            self.assertEqual(expected_sql, str(query_cls.from_(table).select(field)), query_cls.__name__)


class ValueBackslashEscapingTests(TestCase):
    # MySQL and ClickHouse treat a backslash as an escape character in string
    # literals by default, so values are escaped to round-trip; other dialects
    # keep master's literal rendering. NO_BACKSLASH_ESCAPES server modes cannot
    # be detected client-side, so escaping follows the default mode.

    def assert_value_rendering(self, value, expected_by_dialect):
        table = Table("t")
        for query_cls, expected_sql in expected_by_dialect.items():
            query = query_cls.update(table).set(table.foo, value)
            self.assertEqual(expected_sql, str(query), query_cls.__name__)

    def test_trailing_backslash(self):
        self.assert_value_rendering(
            "bar\\",
            {
                MySQLQuery: "UPDATE `t` SET `foo`='bar\\\\'",
                ClickHouseQuery: 'ALTER TABLE "t" UPDATE "foo"=\'bar\\\\\'',
                Query: "UPDATE \"t\" SET \"foo\"='bar\\'",
                PostgreSQLQuery: "UPDATE \"t\" SET \"foo\"='bar\\'",
                SQLLiteQuery: "UPDATE \"t\" SET \"foo\"='bar\\'",
                MSSQLQuery: "UPDATE \"t\" SET \"foo\"='bar\\'",
            },
        )

    def test_backslash_and_quote_char(self):
        self.assert_value_rendering(
            "it\\'s",
            {
                MySQLQuery: "UPDATE `t` SET `foo`='it\\\\''s'",
                ClickHouseQuery: "ALTER TABLE \"t\" UPDATE \"foo\"='it\\\\''s'",
                Query: "UPDATE \"t\" SET \"foo\"='it\\''s'",
                PostgreSQLQuery: "UPDATE \"t\" SET \"foo\"='it\\''s'",
                SQLLiteQuery: "UPDATE \"t\" SET \"foo\"='it\\''s'",
                MSSQLQuery: "UPDATE \"t\" SET \"foo\"='it\\''s'",
            },
        )


class JSONStringEscapingTests(TestCase):
    def test_double_quotes_in_value(self):
        query = PostgreSQLQuery.select(JSON(['say "hello"']))

        self.assertEqual(r"""SELECT '["say \"hello\""]'""", str(query))

    def test_backslashes_in_value(self):
        query = PostgreSQLQuery.select(JSON(["C:\\tmp\\"]))

        self.assertEqual(r"""SELECT '["C:\\tmp\\"]'""", str(query))

    def test_control_characters_in_value(self):
        query = PostgreSQLQuery.select(JSON("\b\f\n\r\t\x00\x01\x1f"))

        self.assertEqual(r"""SELECT '"\b\f\n\r\t\u0000\u0001\u001f"'""", str(query))

    def test_nested_dictionary_keys_and_values(self):
        query = PostgreSQLQuery.select(JSON({'a"b': [{"c\\d": "it's\nready"}]}))

        self.assertEqual(r"""SELECT '{"a\"b":[{"c\\d":"it''s\nready"}]}'""", str(query))

    def test_apostrophes_are_escaped_for_sql(self):
        query = PostgreSQLQuery.select(JSON(["Scrooge Mc'Duck"]))

        self.assertEqual("""SELECT '["Scrooge Mc''Duck"]'""", str(query))

    def test_unicode_is_preserved(self):
        query = PostgreSQLQuery.select(JSON({"中文": "café 😀"}))

        self.assertEqual("""SELECT '{"中文":"café 😀"}'""", str(query))

    def test_alias_is_preserved(self):
        query = PostgreSQLQuery.select(JSON('say "hello"').as_('payload'))

        self.assertEqual(r'''SELECT '"say \"hello\""' "payload"''', str(query))

    def test_json_escapes_are_preserved_by_sql_dialect(self):
        value = 'a"b\\c\n'
        for query_cls in (Query, PostgreSQLQuery, SQLLiteQuery, MSSQLQuery):
            with self.subTest(dialect=query_cls.__name__):
                self.assertEqual(r"""SELECT '"a\"b\\c\n"'""", str(query_cls.select(JSON(value))))
        for query_cls in (MySQLQuery, ClickHouseQuery):
            with self.subTest(dialect=query_cls.__name__):
                self.assertEqual(r"""SELECT '"a\\"b\\\\c\\n"'""", str(query_cls.select(JSON(value))))
