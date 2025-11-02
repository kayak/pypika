import unittest

from pypika import Column, Columns, Query, Table, Tables
from pypika.enums import ReferenceOption, SqlTypes
from pypika.terms import Index, ValueWrapper


class CreateTableTests(unittest.TestCase):
    new_table, existing_table = Tables("abc", "efg")
    foo, bar = Columns(("a", "INT"), ("b", "VARCHAR(100)"))

    def test_create_table_with_columns(self):
        with self.subTest("with nullable"):
            a = Column("a", "INT", True)
            b = Column("b", "VARCHAR(100)", False)
            q = Query.create_table(self.new_table).columns(a, b)

            self.assertEqual('CREATE TABLE "abc" ("a" INT NULL,"b" VARCHAR(100) NOT NULL)', str(q))

        with self.subTest("with defaults"):
            a = Column("a", "INT", default=ValueWrapper(42))
            b = Column("b", "VARCHAR(100)", default=ValueWrapper("foo"))
            q = Query.create_table(self.new_table).columns(a, b)

            self.assertEqual('CREATE TABLE "abc" ("a" INT DEFAULT 42,"b" VARCHAR(100) DEFAULT \'foo\')', str(q))

        with self.subTest("with unwrapped defaults"):
            a = Column("a", "INT", default=42)
            b = Column("b", "VARCHAR(100)", default="foo")
            q = Query.create_table(self.new_table).columns(a, b)

            self.assertEqual('CREATE TABLE "abc" ("a" INT DEFAULT 42,"b" VARCHAR(100) DEFAULT \'foo\')', str(q))

        with self.subTest("with period for"):
            a = Column("id", "INT")
            b = Column("valid_from", "DATETIME")
            c = Column("valid_to", "DATETIME")
            q = Query.create_table(self.new_table).columns(a, b, c).period_for('valid_period', b, c)

            self.assertEqual(
                'CREATE TABLE "abc" ('
                '"id" INT,'
                '"valid_from" DATETIME,'
                '"valid_to" DATETIME,'
                'PERIOD FOR "valid_period" ("valid_from","valid_to"))',
                str(q),
            )

        with self.subTest("without temporary keyword"):
            q = Query.create_table(self.new_table).columns(self.foo, self.bar)

            self.assertEqual('CREATE TABLE "abc" ("a" INT,"b" VARCHAR(100))', str(q))

        with self.subTest("with temporary keyword"):
            q = Query.create_table(self.new_table).temporary().columns(self.foo, self.bar)

            self.assertEqual('CREATE TEMPORARY TABLE "abc" ("a" INT,"b" VARCHAR(100))', str(q))

        with self.subTest("with primary key"):
            q = Query.create_table(self.new_table).columns(self.foo, self.bar).primary_key(self.foo, self.bar)

            self.assertEqual('CREATE TABLE "abc" ("a" INT,"b" VARCHAR(100),PRIMARY KEY ("a","b"))', str(q))

        with self.subTest("with simple foreign key"):
            cref, dref = Columns(("c", "INT"), ("d", "VARCHAR(100)"))
            q = (
                Query.create_table(self.new_table)
                .columns(self.foo, self.bar)
                .foreign_key([self.foo, self.bar], self.existing_table, [cref, dref])
            )

            self.assertEqual(
                'CREATE TABLE "abc" ('
                '"a" INT,'
                '"b" VARCHAR(100),'
                'FOREIGN KEY ("a","b") REFERENCES "efg" ("c","d"))',
                str(q),
            )

        with self.subTest("with foreign key reference options"):
            cref, dref = Columns(("c", "INT"), ("d", "VARCHAR(100)"))
            q = (
                Query.create_table(self.new_table)
                .columns(self.foo, self.bar)
                .foreign_key(
                    [self.foo, self.bar],
                    self.existing_table,
                    [cref, dref],
                    on_delete=ReferenceOption.cascade,
                    on_update=ReferenceOption.restrict,
                )
            )

            self.assertEqual(
                'CREATE TABLE "abc" ('
                '"a" INT,'
                '"b" VARCHAR(100),'
                'FOREIGN KEY ("a","b") REFERENCES "efg" ("c","d") ON DELETE CASCADE ON UPDATE RESTRICT)',
                str(q),
            )

        with self.subTest("with unique keys"):
            q = (
                Query.create_table(self.new_table)
                .columns(self.foo, self.bar)
                .unique(self.foo, self.bar)
                .unique(self.foo)
            )

            self.assertEqual('CREATE TABLE "abc" ("a" INT,"b" VARCHAR(100),UNIQUE ("a","b"),UNIQUE ("a"))', str(q))

        with self.subTest("with system versioning"):
            q = Query.create_table(self.new_table).columns(self.foo, self.bar).with_system_versioning()

            self.assertEqual('CREATE TABLE "abc" ("a" INT,"b" VARCHAR(100)) WITH SYSTEM VERSIONING', str(q))

        with self.subTest("with unlogged keyword"):
            q = Query.create_table(self.new_table).unlogged().columns(self.foo, self.bar)

            self.assertEqual('CREATE UNLOGGED TABLE "abc" ("a" INT,"b" VARCHAR(100))', str(q))

        with self.subTest("with if not exists keyword"):
            q = Query.create_table(self.new_table).if_not_exists().columns(self.foo, self.bar)

            self.assertEqual('CREATE TABLE IF NOT EXISTS "abc" ("a" INT,"b" VARCHAR(100))', str(q))

        with self.subTest("with primary key & auto increment"):
            a = Column("a", SqlTypes.INTEGER_AUTO_INCREMENT, False)
            b = Column("b", "VARCHAR(100)", default="foo")
            q = Query.create_table(self.new_table).columns(a, b).primary_key(a)

            self.assertEqual('CREATE TABLE "abc" ("a" INTEGER AUTO_INCREMENT NOT NULL,"b" VARCHAR(100) DEFAULT \'foo\',PRIMARY KEY ("a"))', str(q))

    def test_create_table_with_select(self):
        select = Query.from_(self.existing_table).select(self.existing_table.foo, self.existing_table.bar)

        with self.subTest("without temporary keyword"):
            q = Query.create_table(self.new_table).as_select(select)

            self.assertEqual('CREATE TABLE "abc" AS (SELECT "foo","bar" FROM "efg")', str(q))

        with self.subTest("with temporary keyword"):
            q = Query.create_table(self.new_table).temporary().as_select(select)

            self.assertEqual(
                'CREATE TEMPORARY TABLE "abc" AS (SELECT "foo","bar" FROM "efg")',
                str(q),
            )

    def test_create_table_without_columns_or_select_empty(self):
        q = Query.create_table(self.new_table)

        self.assertEqual("", str(q))

    def test_create_table_with_select_and_columns_fails(self):
        select = Query.from_(self.existing_table).select(self.existing_table.foo, self.existing_table.bar)

        with self.subTest("for columns before as_select"):
            with self.assertRaises(AttributeError):
                Query.create_table(self.new_table).columns(self.foo, self.bar).as_select(select)

        with self.subTest("for as_select before columns"):
            with self.assertRaises(AttributeError):
                Query.create_table(self.new_table).as_select(select).columns(self.foo, self.bar)

        with self.subTest("repeated foreign key"):
            with self.assertRaises(AttributeError):
                Query.create_table(self.new_table).foreign_key([self.foo], self.existing_table, [self.bar]).foreign_key(
                    [self.foo], self.existing_table, [self.bar]
                )

    def test_create_table_as_select_not_query_raises_error(self):
        with self.assertRaises(TypeError):
            Query.create_table(self.new_table).as_select("abc")


class CreateIndexTests(unittest.TestCase):
    new_index = Index("abc")
    new_table = Table("my_table")
    columns = (new_table.a, new_table.b)
    where = new_table.a > 1

    def test_create_index_simple(self) -> None:
        q = Query.create_index(self.new_index).on(self.new_table).columns(*self.columns)
        self.assertEqual(
            f'CREATE INDEX "{self.new_index.name}" ON "{self.new_table.get_table_name()}"({", ".join([c.name for c in self.columns])})',
            q.get_sql(),
        )

    def test_create_index_where(self) -> None:
        q = Query.create_index(self.new_index).on(self.new_table).columns(*self.columns).where(self.where)
        self.assertEqual(
            f'CREATE INDEX "{self.new_index.name}" ON "{self.new_table.get_table_name()}"({", ".join([c.name for c in self.columns])}) WHERE {self.where.get_sql()}',
            q.get_sql(),
        )

    def test_create_index_unique(self) -> None:
        q = Query.create_index(self.new_index).on(self.new_table).columns(*self.columns).unique()
        self.assertEqual(
            f'CREATE UNIQUE INDEX "{self.new_index.name}" ON "{self.new_table.get_table_name()}"({", ".join([c.name for c in self.columns])})',
            q.get_sql(),
        )

    def test_create_index_if_not_exists(self) -> None:
        q = Query.create_index(self.new_index).on(self.new_table).columns(*self.columns).if_not_exists()
        self.assertEqual(
            f'CREATE INDEX IF NOT EXISTS "{self.new_index.name}" ON "{self.new_table.get_table_name()}"({", ".join([c.name for c in self.columns])})',
            q.get_sql(),
        )

    def test_create_index_without_columns_raises_error(self) -> None:
        with self.assertRaises(AttributeError):
            Query.create_index(self.new_index).on(self.new_table).get_sql()

    def test_create_index_without_table_raises_error(self) -> None:
        with self.assertRaises(AttributeError):
            Query.create_index(self.new_index).columns(*self.columns).get_sql()
