import unittest

from pypika.dialects import JiraQueryBuilder, JiraTable


class SelectTests(unittest.TestCase):
    table_abc = JiraTable()

    def test_in_query(self):
        q = JiraQueryBuilder().where(self.table_abc.project.isin(["PROJ1", "PROJ2"]))

        self.assertEqual('project IN ("PROJ1","PROJ2")', str(q))

    def test_eq_query(self):
        q = JiraQueryBuilder().where(self.table_abc.issuetype == "My issue")

        self.assertEqual('issuetype="My issue"', str(q))

    def test_or_query(self):
        q = JiraQueryBuilder().where(
            self.table_abc.labels.isempty() | self.table_abc.labels.notin(["stale", "bug fix"])
        )

        self.assertEqual('labels is EMPTY OR labels NOT IN ("stale","bug fix")', str(q))

    def test_and_query(self):
        q = JiraQueryBuilder().where(self.table_abc.repos.notempty() & self.table_abc.repos.notin(["main", "dev"]))

        self.assertEqual('repos is not EMPTY AND repos NOT IN ("main","dev")', str(q))
