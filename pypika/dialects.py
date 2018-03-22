from .utils import builder
from .enums import Dialects
from .queries import (
    Query,
    QueryBuilder,
)


class MySQLQuery(Query):
    """
    Defines a query class for use with MySQL.
    """

    @classmethod
    def _builder(cls):
        return QueryBuilder(quote_char='`', dialect=Dialects.MYSQL, wrap_union_queries=False)


class VerticaQueryBuilder(QueryBuilder):
    def __init__(self):
        super(VerticaQueryBuilder, self).__init__(dialect=Dialects.VERTICA)
        self._hint = None

    @builder
    def hint(self, label):
        self._hint = label

    def get_sql(self, *args, **kwargs):
        sql = super(VerticaQueryBuilder, self).get_sql(*args, **kwargs)

        if self._hint is not None:
            sql = ''.join([sql[:7],
                           '/*+label({hint})*/'.format(hint=self._hint),
                           sql[6:]])

        return sql


class VerticaQuery(Query):
    """
    Defines a query class for use with Vertica.
    """

    @classmethod
    def _builder(cls):
        return VerticaQueryBuilder()


class OracleQuery(Query):
    """
    Defines a query class for use with Oracle.
    """

    @classmethod
    def _builder(cls):
        return QueryBuilder(dialect=Dialects.ORACLE)


class PostgreSQLQuery(Query):
    """
    Defines a query class for use with PostgreSQL.
    """

    @classmethod
    def _builder(cls):
        return QueryBuilder(dialect=Dialects.POSTGRESQL)


class RedshiftQuery(Query):
    """
    Defines a query class for use with Amazon Redshift.
    """

    @classmethod
    def _builder(cls):
        return QueryBuilder(dialect=Dialects.REDSHIFT)


class MSSQLQuery(Query):
    """
    Defines a query class for use with Microsoft SQL Server.
    """

    @classmethod
    def _builder(cls):
        return QueryBuilder(dialect=Dialects.MSSQL)


class ClickHouseQuery(Query):
    """
    Defines a query class for use with Yandex ClickHouse.
    """

    @classmethod
    def _builder(cls):
        return QueryBuilder(dialect=Dialects.CLICKHOUSE, wrap_union_queries=False)
