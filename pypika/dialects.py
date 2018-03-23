from pypika.terms import ArithmeticExpression, Function, Field, Star
from pypika.utils import builder, QueryException
from pypika.enums import Dialects
from pypika.queries import Query, QueryBuilder


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


class PostgreQueryBuilder(QueryBuilder):
    def __init__(self):
        super(PostgreQueryBuilder, self).__init__(dialect=Dialects.POSTGRESQL)
        self._returns = []
        self._return_star = False

    @builder
    def returning(self, *terms):
        for term in terms:
            if isinstance(term, Field):
                self._return_field(term)
            elif isinstance(term, str):
                self._return_field_str(term)
            elif isinstance(term, ArithmeticExpression):
                self._return_other(term)
            elif isinstance(term, Function):
                raise QueryException('Aggregate functions are not allowed in returning')
            else:
                self._return_other(self._wrap(term))

    def _validate_returning_term(self, term):
        for field in term.fields():
            if not any([self._insert_table, self._update_table, self._delete_from]):
                raise QueryException('Returning can\'t be used in this query')
            if (
                    field.table not in {self._insert_table, self._update_table}
                    and term not in self._from
            ):
                raise QueryException('You can\'t return from other tables')

    def _set_returns_for_star(self):
        self._returns = [returning
                         for returning in self._returns
                         if not hasattr(returning, 'table')]
        self._return_star = True

    def _return_field(self, term):
        if self._return_star:
            # Do not add select terms after a star is selected
            return

        self._validate_returning_term(term)

        if isinstance(term, Star):
            self._set_returns_for_star()

        self._returns.append(term)

    def _return_field_str(self, term):
        if term == '*':
            self._set_returns_for_star()
            self._returns.append(Star())
            return

        if self._insert_table:
            self._return_field(Field(term, table=self._insert_table))
        elif self._update_table:
            self._return_field(Field(term, table=self._update_table))
        elif self._delete_from:
            self._return_field(Field(term, table=self._from[0]))
        else:
            raise QueryException('Returning can\'t be used in this query')

    def _return_other(self, function):
        self._validate_returning_term(function)
        self._returns.append(function)

    def _returning_sql(self, **kwargs):
        return ' RETURNING {returning}'.format(
            returning=','.join(term.get_sql(with_alias=True, **kwargs)
                               for term in self._returns),
        )

    def get_sql(self, with_alias=False, subquery=False, **kwargs):
        querystring = super(PostgreQueryBuilder, self).get_sql(with_alias, subquery, **kwargs)
        if self._returns:
            querystring += self._returning_sql()
        return querystring


class PostgreSQLQuery(Query):
    """
    Defines a query class for use with PostgreSQL.
    """

    @classmethod
    def _builder(cls):
        return PostgreQueryBuilder()


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
