from unittest import TestCase

from pypika import Query
from pypika.terms import AtTimezone


class AtTimezoneTests(TestCase):
    def test_when_interval_not_specified(self):
        query = Query.from_('customers').select(AtTimezone('date', 'US/Eastern'))
        self.assertEqual('SELECT date AT TIME ZONE \'US/Eastern\' FROM "customers"', str(query))

    def test_when_interval_specified(self):
        query = Query.from_('customers').select(AtTimezone('date', '-06:00', interval=True))
        self.assertEqual('SELECT date AT TIME ZONE INTERVAL \'-06:00\' FROM "customers"', str(query))

    def test_when_alias_specified(self):
        query = Query.from_('customers').select(AtTimezone('date', 'US/Eastern', alias='alias1'))
        self.assertEqual('SELECT date AT TIME ZONE \'US/Eastern\' "alias1" FROM "customers"', str(query))
