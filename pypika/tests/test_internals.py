import unittest
from abc import ABC, abstractmethod
from copy import copy, deepcopy

from pypika import (
    AliasedQuery,
    Case,
    Database,
    Field,
    Query,
    Schema,
    Table,
)
from pypika.terms import Star


class TablesTests(unittest.TestCase):
    def test__criterion_replace_table_with_value(self):
        table = Table("a")

        c = (Field("foo") > 1).replace_table(None, table)
        self.assertEqual(c.left, table)
        self.assertEqual(c.tables_, {table})

    def test__case_tables(self):
        table = Table("a")

        c = Case().when(table.a == 1, 2 * table.a)
        self.assertIsInstance(c.tables_, set)
        self.assertSetEqual(c.tables_, {table})

    def test__star_tables(self):
        star = Star()

        self.assertEqual(star.tables_, set())

    def test__table_star_tables(self):
        table = Table("a")
        star = Star(table=table)

        self.assertEqual(star.tables_, {table})


class AbstractClassAttributeTests(unittest.TestCase):
    def setUp(self):
        self.objects = (
            Query.from_("messages").select("id"),
            Table("messages"),
            AliasedQuery("messages"),
            Schema("public"),
            Database("app"),
        )

    def test_sql_objects_as_abstract_base_class_attributes(self):
        for obj in self.objects:
            with self.subTest(type=type(obj).__name__):

                class Repository(ABC):
                    resource = obj

                self.assertIs(Repository().resource, obj)

    def test_sql_objects_implement_abstract_properties(self):
        class AbstractRepository(ABC):
            @property
            @abstractmethod
            def resource(self):
                pass

        for obj in self.objects:
            with self.subTest(type=type(obj).__name__):

                class Repository(AbstractRepository):
                    resource = obj

                self.assertIs(Repository().resource, obj)

    def test_real_abstract_methods_remain_abstract(self):
        for obj in self.objects:
            with self.subTest(type=type(obj).__name__):

                class Repository(ABC):
                    resource = obj

                    @abstractmethod
                    def execute(self):
                        pass

                self.assertEqual(frozenset({"execute"}), Repository.__abstractmethods__)
                with self.assertRaises(TypeError):
                    Repository()

    def test_explicit_abstract_marker_is_preserved(self):
        class AbstractTable(Table):
            __isabstractmethod__ = True

        class Repository(ABC):
            resource = AbstractTable("messages")

        with self.assertRaises(TypeError):
            Repository()

    def test_copy_preserves_sql_objects(self):
        for obj in self.objects:
            for copy_method in (copy, deepcopy):
                with self.subTest(type=type(obj).__name__, copy=copy_method.__name__):
                    cloned = copy_method(obj)
                    self.assertIsNot(cloned, obj)
                    self.assertEqual(obj.get_sql(), cloned.get_sql())


class DynamicIdentifierTests(unittest.TestCase):
    def setUp(self):
        self.selectables = (
            (Query.from_("messages").select("id").as_("q"), "q"),
            (Table("messages"), "messages"),
            (AliasedQuery("messages"), "messages"),
        )

    def test_dynamic_fields_remain_accessible(self):
        for selectable, namespace in self.selectables:
            for name in ("id", "_private", "__custom__"):
                with self.subTest(type=type(selectable).__name__, name=name):
                    for field in (getattr(selectable, name), selectable[name], selectable.field(name)):
                        self.assertIs(field.table, selectable)
                        self.assertEqual(
                            '"{}"."{}"'.format(namespace, name),
                            field.get_sql(quote_char='"', with_namespace=True),
                        )

    def test_explicit_abstract_marker_field_remains_accessible(self):
        for selectable, namespace in self.selectables:
            with self.subTest(type=type(selectable).__name__):
                for field in (selectable["__isabstractmethod__"], selectable.field("__isabstractmethod__")):
                    self.assertIs(field.table, selectable)
                    self.assertEqual(
                        '"{}"."__isabstractmethod__"'.format(namespace),
                        field.get_sql(quote_char='"', with_namespace=True),
                    )

    def test_dynamic_schema_table_and_field_sql(self):
        table = Database("app").public.messages
        query = Query.from_(table).select(table.id, table["__isabstractmethod__"])

        self.assertEqual('SELECT "id","__isabstractmethod__" FROM "app"."public"."messages"', str(query))

    def test_other_dunder_schema_and_table_names_remain_accessible(self):
        table = Database("app").__custom_schema__.__custom_table__
        query = Query.from_(table).select(table.__custom_field__)

        self.assertEqual('SELECT "__custom_field__" FROM "app"."__custom_schema__"."__custom_table__"', str(query))
