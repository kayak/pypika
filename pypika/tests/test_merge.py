import unittest

from pypika import MergeQueryBuilder, Query, QueryException, Table


class MergeTests(unittest.TestCase):
    target, source = Table("target"), Table("source")

    def merge(self):
        return Query.merge_into(self.target).using(self.source).on(self.target.id == self.source.id)

    def test_update_when_matched(self):
        q = self.merge().when_matched().then_update(self.target.name, self.source.name)

        self.assertEqual(
            'MERGE INTO "target" USING "source" ON ("target"."id"="source"."id") '
            'WHEN MATCHED THEN UPDATE SET "name"="source"."name"',
            str(q),
        )

    def test_insert_when_not_matched(self):
        q = self.merge().when_not_matched().then_insert(("id", "name"), (self.source.id, self.source.name))

        self.assertEqual(
            'MERGE INTO "target" USING "source" ON ("target"."id"="source"."id") '
            'WHEN NOT MATCHED THEN INSERT ("id","name") VALUES ("source"."id","source"."name")',
            str(q),
        )

    def test_insert_without_columns(self):
        q = self.merge().when_not_matched().then_insert(self.source.id, self.source.name)

        self.assertEqual(
            'MERGE INTO "target" USING "source" ON ("target"."id"="source"."id") '
            'WHEN NOT MATCHED THEN INSERT VALUES ("source"."id","source"."name")',
            str(q),
        )

    def test_insert_columns_as_fields(self):
        q = self.merge().when_not_matched().then_insert([self.target.id], [self.source.id])

        self.assertIn('THEN INSERT ("id") VALUES ("source"."id")', str(q))

    def test_delete_when_matched(self):
        q = self.merge().when_matched().then_delete()

        self.assertTrue(str(q).endswith('WHEN MATCHED THEN DELETE'))

    def test_do_nothing(self):
        q = self.merge().when_matched().then_do_nothing()

        self.assertTrue(str(q).endswith('WHEN MATCHED THEN DO NOTHING'))

    def test_update_and_insert(self):
        q = (
            self.merge()
            .when_matched()
            .then_update(self.target.name, self.source.name)
            .when_not_matched()
            .then_insert(("id", "name"), (self.source.id, self.source.name))
        )

        self.assertEqual(
            'MERGE INTO "target" USING "source" ON ("target"."id"="source"."id") '
            'WHEN MATCHED THEN UPDATE SET "name"="source"."name" '
            'WHEN NOT MATCHED THEN INSERT ("id","name") VALUES ("source"."id","source"."name")',
            str(q),
        )

    def test_multiple_updates_share_one_clause(self):
        q = (
            self.merge()
            .when_matched()
            .then_update(self.target.name, self.source.name)
            .then_update(self.target.age, self.source.age)
        )

        self.assertTrue(str(q).endswith('WHEN MATCHED THEN UPDATE SET "name"="source"."name","age"="source"."age"'))

    def test_update_with_string_column_and_constant_value(self):
        q = self.merge().when_matched().then_update("status", "synced").then_update("version", 2)

        self.assertTrue(str(q).endswith('THEN UPDATE SET "status"=\'synced\',"version"=2'))

    def test_table_and_source_given_as_strings(self):
        q = Query.merge_into("target").using("source").on(self.target.id == self.source.id).when_matched().then_delete()

        self.assertEqual(
            'MERGE INTO "target" USING "source" ON ("target"."id"="source"."id") WHEN MATCHED THEN DELETE', str(q)
        )

    def test_matched_with_condition(self):
        q = self.merge().when_matched().and_(self.target.status == "active").then_delete()

        self.assertTrue(str(q).endswith('WHEN MATCHED AND "target"."status"=\'active\' THEN DELETE'))

    def test_not_matched_with_condition(self):
        q = self.merge().when_not_matched().and_(self.source.type == "new").then_insert(self.source.id)

        self.assertTrue(
            str(q).endswith('WHEN NOT MATCHED AND "source"."type"=\'new\' THEN INSERT VALUES ("source"."id")')
        )

    def test_not_matched_by_source(self):
        q = self.merge().when_not_matched_by_source().and_(self.target.status == "obsolete").then_delete()

        self.assertTrue(str(q).endswith('WHEN NOT MATCHED BY SOURCE AND "target"."status"=\'obsolete\' THEN DELETE'))

    def test_not_matched_by_source_update(self):
        q = self.merge().when_not_matched_by_source().then_update(self.target.active, False)

        self.assertTrue(str(q).endswith('WHEN NOT MATCHED BY SOURCE THEN UPDATE SET "active"=false'))

    def test_clauses_with_different_conditions_stay_separate(self):
        q = (
            self.merge()
            .when_matched()
            .and_(self.target.status == "active")
            .then_update(self.target.name, self.source.name)
            .when_matched()
            .then_update(self.target.age, self.source.age)
        )

        self.assertTrue(
            str(q).endswith(
                'WHEN MATCHED AND "target"."status"=\'active\' THEN UPDATE SET "name"="source"."name" '
                'WHEN MATCHED THEN UPDATE SET "age"="source"."age"'
            )
        )

    def test_condition_is_not_carried_to_next_clause(self):
        q = (
            self.merge()
            .when_matched()
            .and_(self.target.status == "active")
            .then_delete()
            .when_matched()
            .then_update(self.target.name, self.source.name)
        )

        self.assertTrue(str(q).endswith('WHEN MATCHED THEN UPDATE SET "name"="source"."name"'))

    def test_subquery_source_is_wrapped_and_aliased(self):
        source = Query.from_("raw").select("id", "name").as_("src")
        q = (
            Query.merge_into(self.target)
            .using(source)
            .on(self.target.id == source.id)
            .when_matched()
            .then_update(self.target.name, source.name)
        )

        self.assertEqual(
            'MERGE INTO "target" USING (SELECT "id","name" FROM "raw") "src" '
            'ON ("target"."id"="src"."id") WHEN MATCHED THEN UPDATE SET "name"="src"."name"',
            str(q),
        )

    def test_aliased_tables(self):
        target, source = Table("target").as_("t"), Table("source").as_("s")
        q = (
            Query.merge_into(target)
            .using(source)
            .on(target.id == source.id)
            .when_matched()
            .then_update(target.name, source.name)
        )

        self.assertEqual(
            'MERGE INTO "target" "t" USING "source" "s" ON ("t"."id"="s"."id") '
            'WHEN MATCHED THEN UPDATE SET "name"="s"."name"',
            str(q),
        )

    def test_builder_is_exported(self):
        self.assertIsInstance(Query.merge_into(self.target), MergeQueryBuilder)


class MergeValidationTests(unittest.TestCase):
    target, source = Table("target"), Table("source")

    def merge(self):
        return Query.merge_into(self.target).using(self.source).on(self.target.id == self.source.id)

    def test_target_is_required(self):
        with self.assertRaisesRegex(QueryException, "target table"):
            str(MergeQueryBuilder())

    def test_source_is_required(self):
        with self.assertRaisesRegex(QueryException, "source"):
            str(Query.merge_into(self.target))

    def test_on_is_required(self):
        with self.assertRaisesRegex(QueryException, "ON criterion"):
            str(Query.merge_into(self.target).using(self.source))

    def test_a_when_clause_is_required(self):
        with self.assertRaisesRegex(QueryException, "WHEN clause"):
            str(self.merge())

    def test_a_started_clause_without_action_is_not_a_clause(self):
        with self.assertRaisesRegex(QueryException, "WHEN clause"):
            str(self.merge().when_matched())

    def test_target_can_only_be_set_once(self):
        with self.assertRaises(QueryException):
            Query.merge_into(self.target).merge_into("other")

    def test_source_can_only_be_set_once(self):
        with self.assertRaises(QueryException):
            Query.merge_into(self.target).using(self.source).using("other")

    def test_on_can_only_be_set_once(self):
        with self.assertRaises(QueryException):
            self.merge().on(self.target.name == self.source.name)

    def test_actions_need_a_when_clause(self):
        q = self.merge()

        with self.assertRaisesRegex(QueryException, "when_matched"):
            q.then_update(self.target.name, self.source.name)
        with self.assertRaisesRegex(QueryException, "when_matched"):
            q.then_delete()
        with self.assertRaisesRegex(QueryException, "when_not_matched"):
            q.then_insert(self.source.id)
        with self.assertRaisesRegex(QueryException, "when_matched"):
            q.then_do_nothing()
        with self.assertRaisesRegex(QueryException, "when_matched"):
            q.and_(self.target.status == "active")

    def test_insert_is_only_allowed_when_not_matched(self):
        for q in (self.merge().when_matched(), self.merge().when_not_matched_by_source()):
            with self.assertRaisesRegex(QueryException, "when_not_matched"):
                q.then_insert(self.source.id)

    def test_update_and_delete_are_not_allowed_when_not_matched(self):
        q = self.merge().when_not_matched()

        with self.assertRaises(QueryException):
            q.then_update(self.target.name, self.source.name)
        with self.assertRaises(QueryException):
            q.then_delete()

    def test_action_finishes_the_clause(self):
        q = self.merge().when_matched().then_delete()

        with self.assertRaises(QueryException):
            q.then_update(self.target.name, self.source.name)


class MergeImmutabilityTests(unittest.TestCase):
    target, source = Table("target"), Table("source")

    def test_builder_methods_return_new_instances(self):
        base = Query.merge_into(self.target).using(self.source).on(self.target.id == self.source.id)
        updating = base.when_matched().then_update(self.target.name, self.source.name)
        deleting = base.when_matched().then_delete()

        self.assertIsNot(base, updating)
        self.assertTrue(str(updating).endswith("THEN UPDATE SET \"name\"=\"source\".\"name\""))
        self.assertTrue(str(deleting).endswith("THEN DELETE"))
        with self.assertRaises(QueryException):
            str(base)

    def test_extending_a_clause_does_not_change_the_original(self):
        first = (
            Query.merge_into(self.target)
            .using(self.source)
            .on(self.target.id == self.source.id)
            .when_matched()
            .then_update(self.target.name, self.source.name)
        )
        before = str(first)
        second = first.then_update(self.target.age, self.source.age)

        self.assertEqual(before, str(first))
        self.assertIn('"age"', str(second))
