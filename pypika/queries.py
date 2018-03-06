# coding: utf8
from pypika.enums import (
    JoinType,
    UnionType,
)
from pypika.utils import (
    JoinException,
    RollupException,
    UnionException,
    alias_sql,
    builder,
    ignoredeepcopy,
)
from .terms import (
    ArithmeticExpression,
    Field,
    Function,
    Rollup,
    Star,
    Term,
    Tuple,
    ValueWrapper,
)

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class Selectable(object):
    def __init__(self, alias):
        self.alias = alias

    def field(self, name):
        return Field(name, table=self)

    @property
    def star(self):
        return Star(self)

    @ignoredeepcopy
    def __getattr__(self, name):
        return self.field(name)


class Table(Selectable):
    def __init__(self, name, schema=None, alias=None):
        super(Table, self).__init__(alias)
        self.table_name = name
        self.schema = schema

    def get_sql(self, quote_char=None, **kwargs):
        # FIXME escape

        if self.schema:
            table_sql = "{quote}{schema}{quote}.{quote}{name}{quote}".format(
                  schema=self.schema,
                  name=self.table_name,
                  quote=quote_char or ''
            )

        else:
            table_sql = "{quote}{name}{quote}".format(
                  name=self.table_name,
                  quote=quote_char or ''
            )

        if self.alias is None:
            return table_sql
        return alias_sql(table_sql, self.alias, quote_char)

    def __str__(self):
        return self.get_sql(quote_char='"')

    def __eq__(self, other):
        if not isinstance(other, Table):
            return False

        if self.table_name != other.table_name:
            return False

        if self.schema != other.schema:
            return False

        if self.alias != other.alias:
            return False

        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(str(self))


def make_tables(*names, **kwargs):
    return [Table(name, schema=kwargs.get('schema')) for name in names]


class Query(object):
    """
    Query is the primary class and entry point in pypika. It is used to build queries iteratively using the builder
    design
    pattern.

    This class is immutable.
    """

    @classmethod
    def _builder(cls):
        return QueryBuilder()

    @classmethod
    def from_(cls, table):
        """
        Query builder entry point.  Initializes query building and sets the table to select from.  When using this
        function, the query becomes a SELECT query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return cls._builder().from_(table)

    @classmethod
    def into(cls, table):
        """
        Query builder entry point.  Initializes query building and sets the table to insert into.  When using this
        function, the query becomes an INSERT query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return cls._builder().into(table)

    @classmethod
    def select(cls, *terms):
        """
        Query builder entry point.  Initializes query building without a table and selects fields.  Useful when testing
        SQL functions.

        :param terms:
            Type: list[expression]

            A list of terms to select.  These can be any type of int, float, str, bool, or Term.  They cannot be a Field
            unless the function ``Query.from_`` is called first.

        :returns QueryBuilder
        """
        return cls._builder().select(*terms)

    @classmethod
    def update(cls, table):
        """
        Query builder entry point.  Initializes query building and sets the table to update.  When using this
        function, the query becomes an UPDATE query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return cls._builder().update(table)


class _UnionQuery(Selectable, Term):
    """
    A Query class wrapper for a Union query, whether DISTINCT or ALL.

    Created via the functionds `Query.union` or `Query.union_all`, this class should not be instantiated directly.
    """
    def __init__(self, base_query, union_query, union_type, alias=None):
        super(_UnionQuery, self).__init__(alias)
        self.base_query = base_query
        self._unions = [(union_type, union_query)]
        self._orderbys = []

        self._limit = None
        self._offset = None

    @builder
    def orderby(self, *fields, **kwargs):
        for field in fields:
            field = (Field(field, table=self.base_query._from[0])
                     if isinstance(field, str)
                     else self.base_query._wrap(field))

            self._orderbys.append((field, kwargs.get('order')))

    @builder
    def limit(self, limit):
        self._limit = limit

    @builder
    def offset(self, offset):
        self._offset = offset

    @builder
    def union(self, other):
        self._unions.append((UnionType.distinct, other))

    @builder
    def union_all(self, other):
        self._unions.append((UnionType.all, other))

    def __str__(self):
        return self.get_sql()

    def get_sql(self, with_alias=False, subquery=False, **kwargs):
        union_template = ' UNION{type} {union}'

        kwargs = {'quote_char': self.base_query.quote_char, 'dialect': self.base_query.dialect}
        base_querystring = self.base_query.get_sql(subquery=self.base_query.wrap_union_queries, **kwargs)

        querystring = base_querystring
        for union_type, union_query in self._unions:
            union_querystring = union_query.get_sql(subquery=self.base_query.wrap_union_queries, **kwargs)

            if len(self.base_query._selects) != len(union_query._selects):
                raise UnionException("Queries must have an equal number of select statements in a union."
                                     "\n\nMain Query:\n{query1}\n\nUnion Query:\n{query2}"
                                     .format(query1=base_querystring, query2=union_querystring))

            querystring += union_template.format(type=union_type.value,
                                                 union=union_querystring)

        if self._orderbys:
            querystring += self._orderby_sql(**kwargs)

        if self._limit:
            querystring += self._limit_sql()

        if self._offset:
            querystring += self._offset_sql()

        if subquery:
            querystring = '({query})'.format(query=querystring)

        if with_alias:
            return alias_sql(querystring, self.alias or self.table_name, kwargs.get('quote_char'))

        return querystring

    def _orderby_sql(self, quote_char=None, **kwargs):
        """
        Produces the ORDER BY part of the query.  This is a list of fields and possibly their directionality, ASC or
        DESC. The clauses are stored in the query under self._orderbys as a list of tuples containing the field and
        directionality (which can be None).

        If an order by field is used in the select clause, determined by a matching , then the ORDER BY clause will use
        the alias, otherwise the field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self.base_query._selects}
        for field, directionality in self._orderbys:
            term = "{quote}{alias}{quote}".format(alias=field.alias, quote=quote_char or '') \
                if field.alias and field.alias in selected_aliases \
                else field.get_sql(quote_char=quote_char, **kwargs)

            clauses.append('{term} {orient}'.format(term=term, orient=directionality.value)
                           if directionality is not None else term)

        return ' ORDER BY {orderby}'.format(orderby=','.join(clauses))

    def _offset_sql(self):
        return " OFFSET {offset}".format(offset=self._offset)

    def _limit_sql(self):
        return " LIMIT {limit}".format(limit=self._limit)


class QueryBuilder(Selectable, Term):
    """
    Query Builder is the main class in pypika which stores the state of a query and offers functions which allow the
    state to be branched immutably.
    """

    def __init__(self, quote_char='"', dialect=None, wrap_union_queries=True):
        super(QueryBuilder, self).__init__(None)

        self._from = []
        self._insert_table = None
        self._update_table = None
        self._delete_from = False

        self._selects = []
        self._columns = []
        self._values = []
        self._distinct = False
        self._ignore = False

        self._wheres = None
        self._prewheres = None
        self._groupbys = []
        self._with_totals = False
        self._havings = None
        self._orderbys = []
        self._joins = []
        self._unions = []

        self._limit = None
        self._offset = None

        self._updates = []

        self._select_star = False
        self._select_star_tables = set()
        self._mysql_rollup = False
        self._select_into = False

        self._subquery_count = 0

        self.quote_char = quote_char
        self.dialect = dialect
        self.wrap_union_queries = wrap_union_queries

    @builder
    def from_(self, selectable):
        """
        Adds a table to the query.  This function can only be called once and will raise an AttributeError if called a
        second time.

        :param selectable:
            Type: ``Table``, ``Query``, or ``str``

            When a ``str`` is passed, a table with the name matching the ``str`` value is used.

        :returns
            A copy of the query with the table added.
        """

        self._from.append(Table(selectable) if isinstance(selectable, str) else selectable)

        if isinstance(selectable, (QueryBuilder, _UnionQuery)) and selectable.alias is None:
            selectable.alias = 'sq%d' % self._subquery_count
            self._subquery_count += 1

    @builder
    def into(self, table):
        if self._insert_table is not None:
            raise AttributeError("'Query' object has no attribute '%s'" % 'into')

        if self._selects:
            self._select_into = True

        self._insert_table = table if isinstance(table, Table) else Table(table)

    @builder
    def select(self, *terms):
        for term in terms:
            if isinstance(term, Field):
                self._select_field(term)
            elif isinstance(term, str):
                self._select_field_str(term)
            elif isinstance(term, (Function, ArithmeticExpression)):
                self._select_other(term)
            else:
                self._select_other(self._wrap(term))

    @builder
    def delete(self):
        if self._delete_from or self._selects or self._update_table:
            raise AttributeError("'Query' object has no attribute '%s'" % 'delete')

        self._delete_from = True

    @builder
    def update(self, table):
        if self._update_table is not None or self._selects or self._delete_from:
            raise AttributeError("'Query' object has no attribute '%s'" % 'update')

        self._update_table = table if isinstance(table, Table) else Table(table)

    @builder
    def columns(self, *terms):
        if self._insert_table is None:
            raise AttributeError("'Query' object has no attribute '%s'" % 'insert')

        for term in terms:
            if isinstance(term, str):
                term = Field(term, table=self._insert_table)
            self._columns.append(term)

    @builder
    def insert(self, *terms):
        if self._insert_table is None:
            raise AttributeError("'Query' object has no attribute '%s'" % 'insert')

        if not terms:
            return

        if not isinstance(terms[0], (list, tuple, set)):
            terms = [terms]

        for values in terms:
            self._values.append([value
                                 if isinstance(value, Term)
                                 else self._wrap(value)
                                 for value in values])

    @builder
    def distinct(self):
        self._distinct = True

    @builder
    def ignore(self):
        self._ignore = True

    @builder
    def prewhere(self, criterion):
        self._validate_term(criterion)

        if self._prewheres:
            self._prewheres &= criterion
        else:
            self._prewheres = criterion

    @builder
    def where(self, criterion):
        self._validate_term(criterion)

        if self._wheres:
            self._wheres &= criterion
        else:
            self._wheres = criterion

    @builder
    def having(self, criterion):
        self._validate_term(criterion)

        if self._havings:
            self._havings &= criterion
        else:
            self._havings = criterion

    @builder
    def groupby(self, *terms):
        for term in terms:
            if isinstance(term, str):
                term = Field(term, table=self._from[0])

            self._validate_term(term)
            self._groupbys.append(term)

    @builder
    def with_totals(self):
        self._with_totals = True

    @builder
    def rollup(self, *terms, **kwargs):
        for_mysql = 'mysql' == kwargs.get('vendor')

        if self._mysql_rollup:
            raise AttributeError("'Query' object has no attribute '%s'" % 'rollup')

        terms = [Tuple(*term) if isinstance(term, (list, tuple, set))
                 else term
                 for term in terms]

        if for_mysql:
            # MySQL rolls up all of the dimensions always
            if not terms and not self._groupbys:
                raise RollupException('At least one group is required. Call Query.groupby(term) or pass'
                                      'as parameter to rollup.')

            self._mysql_rollup = True
            self._groupbys += terms

        elif 0 < len(self._groupbys) and isinstance(self._groupbys[-1], Rollup):
            # If a rollup was added last, then append the new terms to the previous rollup
            self._groupbys[-1].args += terms

        else:
            self._groupbys.append(Rollup(*terms))

    @builder
    def orderby(self, *fields, **kwargs):
        for field in fields:
            field = (Field(field, table=self._from[0])
                     if isinstance(field, str)
                     else self._wrap(field))

            self._orderbys.append((field, kwargs.get('order')))

    @builder
    def join(self, item, how=JoinType.inner):
        if isinstance(item, Table):
            return Joiner(self, item, how, type_label='table')

        elif isinstance(item, QueryBuilder):
            return Joiner(self, item, how, type_label='subquery')

        raise ValueError("Cannot join on type '%s'" % type(item))

    @builder
    def limit(self, limit):
        self._limit = limit

    @builder
    def offset(self, offset):
        self._offset = offset

    @builder
    def union(self, other):
        return _UnionQuery(self, other, UnionType.distinct)

    def union_all(self, other):
        return _UnionQuery(self, other, UnionType.all)

    @builder
    def set(self, field, value):
        field = Field(field) if not isinstance(field, Field) else field
        self._updates.append((field, ValueWrapper(value)))

    def __add__(self, other):
        return self.union(other)

    def __mul__(self, other):
        return self.union_all(other)

    @builder
    def __getitem__(self, item):
        if not isinstance(item, slice):
            raise TypeError("Query' object is not subscriptable")
        self._offset = item.start
        self._limit = item.stop

    @staticmethod
    def _list_aliases(field_set, quote_char=None):
        return [field.alias or field.get_sql(quote_char=quote_char)
                for field in field_set]

    def _select_field_str(self, term):
        if term == '*':
            self._select_star = True
            self._selects = [Star()]
            return

        self._select_field(Field(term, table=self._from[0]))

    def _select_field(self, term):
        if self._select_star:
            # Do not add select terms after a star is selected
            return

        self._validate_term(term)

        if term.table in self._select_star_tables:
            # Do not add select terms for table after a table star is selected
            return

        if isinstance(term, Star):
            self._selects = [select
                             for select in self._selects
                             if not hasattr(select, 'table') or term.table != select.table]
            self._select_star_tables.add(term.table)

        self._selects.append(term)

    def _select_other(self, function):
        self._validate_term(function)
        self._selects.append(function)

    def fields(self):
        # Don't return anything here. Subqueries have their own fields.
        return []

    def do_join(self, join):
        join.validate(self._from, self._joins)

        if isinstance(join.item, QueryBuilder) and join.item.alias is None:
            self._tag_subquery(join.item)

        table_in_from = any(isinstance(clause, Table)
                            and join.item.table_name == self._from[0].table_name
                            for clause in self._from)
        if isinstance(join.item, Table) and join.item.alias is None and table_in_from:
            # On the odd chance that we join the same table as the FROM table and don't set an alias
            # FIXME only works once
            join.item.alias = join.item.table_name + '2'

        self._joins.append(join)

    def _validate_term(self, term):
        for field in term.fields():
            table_in_froms = field.table in self._from
            table_in_joins = field.table in [join.item for join in self._joins]
            if field.table is not None and \
                  not table_in_froms and \
                  not table_in_joins and \
                        field.table != self._update_table:
                raise JoinException('Table [%s] missing from query.  '
                                    'Table must be first joined before any of '
                                    'its fields can be used' % field.table)

    def _tag_subquery(self, subquery):
        subquery.alias = 'sq%d' % self._subquery_count
        self._subquery_count += 1

    def __str__(self):
        return self.get_sql(quote_char=self.quote_char, dialect=self.dialect)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if not isinstance(other, QueryBuilder):
            return False

        if not self.alias == other.alias:
            return False

        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.alias) + sum(hash(clause) for clause in self._from)

    def get_sql(self, with_alias=False, subquery=False, **kwargs):
        if not (self._selects or self._insert_table or self._delete_from or self._update_table):
            return ''
        if self._insert_table and not (self._selects or self._values):
            return ''
        if self._update_table and not self._updates:
            return ''

        has_joins = bool(self._joins)
        has_multiple_from_clauses = 1 < len(self._from)
        has_subquery_from_clause = 0 < len(self._from) and isinstance(self._from[0], QueryBuilder)

        kwargs['with_namespace'] = any((has_joins, has_multiple_from_clauses, has_subquery_from_clause))

        if self._update_table:
            querystring = self._update_sql(**kwargs)
            querystring += self._set_sql(**kwargs)

            if self._wheres:
                querystring += self._where_sql(**kwargs)
            return querystring
        elif self._delete_from:
            querystring = self._delete_sql(**kwargs)
        elif not self._select_into and self._insert_table:
            querystring = self._insert_sql(**kwargs)

            if self._columns:
                querystring += self._columns_sql(**kwargs)

            if self._values:
                return querystring + self._values_sql(**kwargs)
            else:
                querystring += ' ' + self._select_sql(**kwargs)
        else:
            querystring = self._select_sql(**kwargs)

            if self._insert_table:
                querystring += self._into_sql(**kwargs)

        if self._from:
            querystring += self._from_sql(**kwargs)

        if self._joins:
            querystring += " " + " ".join(join.get_sql(**kwargs)
                                          for join in self._joins)

        if self._prewheres:
            querystring += self._prewhere_sql(**kwargs)

        if self._wheres:
            querystring += self._where_sql(**kwargs)

        if self._groupbys:
            querystring += self._group_sql(**kwargs)
            if self._mysql_rollup:
                querystring += self._rollup_sql()

        if self._havings:
            querystring += self._having_sql(**kwargs)

        if self._orderbys:
            querystring += self._orderby_sql(**kwargs)

        if self._limit:
            querystring += self._limit_sql()

        if self._offset:
            querystring += self._offset_sql()

        if subquery:
            querystring = '({query})'.format(query=querystring)

        if with_alias:
            return alias_sql(querystring, self.alias or self.table_name, kwargs.get('quote_char'))

        return querystring

    def _select_sql(self, **kwargs):
        return 'SELECT {distinct}{select}'.format(
              distinct='DISTINCT ' if self._distinct else '',
              select=','.join(term.get_sql(with_alias=True, **kwargs)
                              for term in self._selects),
        )

    def _insert_sql(self, **kwargs):
        return 'INSERT {ignore}INTO {table}'.format(
              table=self._insert_table.get_sql(**kwargs),
              ignore='IGNORE ' if self._ignore else ''
        )

    @staticmethod
    def _delete_sql(**kwargs):
        return 'DELETE'

    def _update_sql(self, **kwargs):
        return 'UPDATE {table}'.format(
              table=self._update_table.get_sql(**kwargs)
        )

    def _columns_sql(self, with_namespace=False, **kwargs):
        """
        SQL for Columns clause for INSERT queries
        :param with_namespace:
            Remove from kwargs, never format the column terms with namespaces since only one table can be inserted into
        """
        return ' ({columns})'.format(
              columns=','.join(term.get_sql(with_namespace=False, **kwargs)
                               for term in self._columns)
        )

    def _values_sql(self, **kwargs):
        return ' VALUES ({values})'.format(
              values='),('.join(','.join(term.get_sql(with_alias=True, **kwargs)
                                         for term in row)
                                for row in self._values)

        )

    def _into_sql(self, **kwargs):
        return ' INTO {table}'.format(
              table=self._insert_table.get_sql(with_alias=False, **kwargs),
        )

    def _from_sql(self, with_namespace=False, **kwargs):
        return ' FROM {selectable}'.format(selectable=','.join(
              clause.get_sql(subquery=True, with_alias=True, **kwargs)
              for clause in self._from
        ))

    def _prewhere_sql(self, quote_char=None, **kwargs):
        return ' PREWHERE {prewhere}'.format(
              prewhere=self._prewheres.get_sql(quote_char=quote_char, subquery=True, **kwargs))

    def _where_sql(self, quote_char=None, **kwargs):
        return ' WHERE {where}'.format(where=self._wheres.get_sql(quote_char=quote_char, subquery=True, **kwargs))

    def _group_sql(self, quote_char=None, **kwargs):
        """
        Produces the GROUP BY part of the query.  This is a list of fields. The clauses are stored in the query under
        self._groupbys as a list fields.

        If an groupby field is used in the select clause, determined by a matching alias, then the GROUP BY clause will
        use the alias, otherwise the entire field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self._selects}
        for field in self._groupbys:
            if field.alias and field.alias in selected_aliases:
                clauses.append("{quote}{alias}{quote}".format(
                      alias=field.alias,
                      quote=quote_char or '',
                ))
            else:
                clauses.append(field.get_sql(quote_char=quote_char, **kwargs))

        sql = ' GROUP BY {groupby}'.format(groupby=','.join(clauses))
        if self._with_totals:
            return sql + ' WITH TOTALS'
        return sql

    def _orderby_sql(self, quote_char=None, **kwargs):
        """
        Produces the ORDER BY part of the query.  This is a list of fields and possibly their directionality, ASC or
        DESC. The clauses are stored in the query under self._orderbys as a list of tuples containing the field and
        directionality (which can be None).

        If an order by field is used in the select clause, determined by a matching , then the ORDER BY clause will use
        the alias, otherwise the field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self._selects}
        for field, directionality in self._orderbys:
            term = "{quote}{alias}{quote}".format(alias=field.alias, quote=quote_char or '') \
                if field.alias and field.alias in selected_aliases \
                else field.get_sql(quote_char=quote_char, **kwargs)

            clauses.append('{term} {orient}'.format(term=term, orient=directionality.value)
                           if directionality is not None else term)

        return ' ORDER BY {orderby}'.format(orderby=','.join(clauses))

    def _rollup_sql(self):
        return ' WITH ROLLUP'

    def _having_sql(self, quote_char=None, **kwargs):
        return ' HAVING {having}'.format(having=self._havings.get_sql(quote_char=quote_char, **kwargs))

    def _offset_sql(self):
        return " OFFSET {offset}".format(offset=self._offset)

    def _limit_sql(self):
        return " LIMIT {limit}".format(limit=self._limit)

    def _set_sql(self, **kwargs):
        return ' SET {set}'.format(
              set=','.join(
                    '{field}={value}'.format(
                          field=field.get_sql(**kwargs),
                          value=value.get_sql(**kwargs)) for field, value in self._updates
              )
        )


class Joiner(object):
    def __init__(self, query, item, how, type_label):
        self.query = query
        self.item = item
        self.how = how
        self.type_label = type_label

    def on(self, criterion):
        if criterion is None:
            raise JoinException("Parameter 'criterion' is required for a "
                                "{type} JOIN but was not supplied.".format(type=self.type_label))

        self.query.do_join(JoinOn(self.item, self.how, criterion))
        return self.query

    def on_field(self, *fields):
        if not fields:
            raise JoinException("Parameter 'fields' is required for a "
                                "{type} JOIN but was not supplied.".format(type=self.type_label))

        criterion = None
        for field in fields:
            consituent = Field(field, table=self.query._from[0]) == Field(field, table=self.item)
            criterion = consituent if criterion is None else criterion & consituent

        self.query.do_join(JoinOn(self.item, self.how, criterion))
        return self.query

    def using(self, *fields):
        if not fields:
            raise JoinException("Parameter 'fields' is required when joining with "
                                "a using clause but was not supplied.".format(type=self.type_label))

        self.query.do_join(JoinUsing(self.item, self.how, [Field(field) for field in fields]))
        return self.query


class Join(object):
    def __init__(self, item, how):
        self.item = item
        self.how = how

    def get_sql(self, **kwargs):
        sql = 'JOIN {table}'.format(
              table=self.item.get_sql(subquery=True, with_alias=True, **kwargs),
        )

        if self.how.value:
            return '{type} {join}'.format(join=sql, type=self.how.value)
        return sql


class JoinOn(Join):
    def __init__(self, item, how, criteria):
        super(JoinOn, self).__init__(item, how)
        self.criterion = criteria

    def get_sql(self, **kwargs):
        join_sql = super(JoinOn, self).get_sql(**kwargs)
        return '{join} ON {criterion}'.format(
              join=join_sql,
              criterion=self.criterion.get_sql(**kwargs),
        )

    def validate(self, _from, _joins):
        criterion_tables = set([f.table for f in self.criterion.fields()])
        available_tables = (set(_from) | {join.item for join in _joins} | {self.item})
        missing_tables = criterion_tables - available_tables
        if missing_tables:
            raise JoinException('Invalid join criterion. One field is required from the joined item and '
                                'another from the selected table or an existing join.  Found [{tables}]'.format(
                  tables=', '.join(map(str, missing_tables))
            ))


class JoinUsing(Join):
    def __init__(self, item, how, fields):
        super(JoinUsing, self).__init__(item, how)
        self.fields = fields

    def get_sql(self, **kwargs):
        join_sql = super(JoinUsing, self).get_sql(**kwargs)
        return '{join} USING ({fields})'.format(
              join=join_sql,
              fields=','.join(str(field) for field in self.fields)
        )

    def validate(self, _from, _joins):
        pass
