import sys
import unittest
import uuid
from ipaddress import IPv4Address, IPv6Address

from pypika import PostgreSQLQuery, QmarkParameter, Query, Table
from pypika.terms import ValueWrapper


class StringTests(unittest.TestCase):
    def test_inline_string_concatentation(self):
        self.assertEqual("'it''s'", ValueWrapper("it's").get_sql())


class UuidTests(unittest.TestCase):
    def test_uuid_string_generation(self):
        id = uuid.uuid4()
        self.assertEqual("'{}'".format(id), ValueWrapper(id).get_sql())


class IpAddressTests(unittest.TestCase):
    def test_ip_address_string_generation(self):
        for address, expected in [
            (IPv4Address('192.0.2.1'), "'192.0.2.1'"),
            (IPv6Address('2001:0db8:0000::1'), "'2001:db8::1'"),
        ]:
            with self.subTest(address=address):
                self.assertEqual(expected, ValueWrapper(address).get_sql())

    @unittest.skipUnless(sys.version_info >= (3, 9), 'Scoped IPv6 requires Python 3.9+')
    def test_scoped_ipv6_string_generation(self):
        for address, expected in [
            (IPv6Address('fe80::1%eth0'), "'fe80::1%eth0'"),
            (IPv6Address("fe80::1%o'hare"), "'fe80::1%o''hare'"),
        ]:
            with self.subTest(address=address):
                self.assertEqual(expected, ValueWrapper(address).get_sql())

    def test_ip_address_secondary_quote_char(self):
        for address in (IPv4Address('192.0.2.1'), IPv6Address('2001:db8::1')):
            with self.subTest(address=address):
                self.assertEqual('"{}"'.format(address), ValueWrapper(address).get_sql(secondary_quote_char='"'))

    def test_ip_address_insert(self):
        table = Table('test_table')
        for address in (IPv4Address('123.123.123.123'), IPv6Address('2001:db8::1')):
            with self.subTest(address=address):
                query = PostgreSQLQuery.into(table).columns('ip').insert(address)
                self.assertEqual('INSERT INTO "test_table" ("ip") VALUES (\'{}\')'.format(address), str(query))

    def test_ip_address_predicate(self):
        table = Table('test_table')
        for address in (IPv4Address('192.0.2.1'), IPv6Address('2001:db8::1')):
            with self.subTest(address=address):
                query = Query.from_(table).select('*').where(table.ip == address)
                self.assertEqual('SELECT * FROM "test_table" WHERE "ip"=\'{}\''.format(address), str(query))

    def test_ip_address_alias(self):
        query = Query.select(ValueWrapper(IPv6Address('2001:db8::1')).as_('ip'))
        self.assertEqual('SELECT \'2001:db8::1\' "ip"', str(query))

    def test_ip_address_parameters_remain_text(self):
        for address in (IPv4Address('192.0.2.1'), IPv6Address('2001:db8::1')):
            with self.subTest(address=address):
                query = Query.into(Table('test_table')).columns('ip').insert(address)
                parameter = QmarkParameter()
                self.assertEqual('INSERT INTO "test_table" ("ip") VALUES (?)', query.get_sql(parameter=parameter))
                self.assertEqual([str(address)], parameter.get_parameters())
