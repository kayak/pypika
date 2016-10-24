# coding: utf8

from pypika.enums import JoinType, UnionType
from pypika.utils import JoinException, UnionException, RollupException
from pypika.utils import builder
from .terms import Field, Star, Term, Function, ArithmeticExpression, Rollup

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

    def __getattr__(self, name):
        # This prevents Fields being when deepcopy functions are called
        if name in ['__deepcopy__', '__getstate__', '__setstate__', '__getnewargs__']:
            raise AttributeError("'Table' object has no attribute '%s'" % name)

        return self.field(name)

        # def __hash__(self):
        #     return hash(str(self))


class Table(Selectable):
    def __init__(self, name, schema=None, alias=None):
        super(Table, self).__init__(alias)
        self.table_name = name
        self.schema = schema

    def get_sql(self, **kwargs):
        # FIXME escape

        if self.schema:
            name = "\"{schema}\".\"{name}\"".format(
                schema=self.schema,
                name=self.table_name,
            )

        else:
            name = "\"{name}\"".format(
                name=self.table_name
            )

        if self.alias:
            return "{name} \"{alias}\"".format(
                name=name,
                alias=self.alias
            )

        return name

    def __str__(self):
        return self.get_sql()

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
    Query is the primary class and entry point in pypika. It is used to build queries iteratively using the builder design
    pattern.

    This class is immutable.
    """

    @staticmethod
    def from_(table):
        """
        Query builder entry point.  Initializes query building and sets the table to select from.  When using this
        function, the query becomes a SELECT query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return QueryBuilder().from_(table)

    @staticmethod
    def into(table):
        """
        Query builder entry point.  Initializes query building and sets the table to insert into.  When using this
        function, the query becomes an INSERT query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return QueryBuilder().into(table)

    @staticmethod
    def select(*terms):
        """
        Query builder entry point.  Initializes query building without a table and selects fields.  Useful when testing
        SQL functions.

        :param terms:
            Type: list[expression]

            A list of terms to select.  These can be any type of int, float, str, bool, or Term.  They cannot be a Field
            unless the function ``Query.from_`` is called first.

        :returns QueryBuilder
        """
        return QueryBuilder().select(*terms)


class QueryBuilder(Selectable, Term):
    """
    Query Builder is the main class in pypika which stores the state of a query and offers functions which allow the
    state to be branched immutably.
    """

    def __init__(self):
        super(QueryBuilder, self).__init__(None)

        self._from = None
        self._insert_table = None

        self._selects = []
        self._columns = []
        self._values = []
        self._distinct = False
        self._ignore = False

        self._wheres = None
        self._groupbys = []
        self._havings = None
        self._orderbys = []
        self._joins = []
        self._unions = []

        self._limit = None
        self._offset = None

        self._select_star = False
        self._select_star_tables = set()
        self._mysql_rollup = False
        self._select_into = False

        self._subquery_count = 0

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
        if self._from is not None:
            raise AttributeError("'Query' object has no attribute '%s'" % 'from_')

        self._from = Table(selectable) if isinstance(selectable, str) else selectable

        if isinstance(selectable, QueryBuilder):
            selectable.table_name = 'sq0'
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
                term = Field(term, table=self._from)

            self._validate_term(term)
            self._groupbys.append(term)

    @builder
    def rollup(self, *terms, **kwargs):
        for_mysql = 'mysql' == kwargs.get('vendor')

        if self._mysql_rollup:
            raise AttributeError("'Query' object has no attribute '%s'" % 'rollup')

        if for_mysql:
            if not terms and not self._groupbys:
                raise RollupException('At least one group is required. Call Query.groupby(term) or pass'
                                      'as parameter to rollup.')

            self._mysql_rollup = True
            self._groupbys += terms

        else:
            self._groupbys.append(Rollup(*terms))

    @builder
    def orderby(self, *fields, **kwargs):
        for field in fields:
            field = (Field(field, table=self._from)
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
        self._unions.append((UnionType.distinct, other))

    @builder
    def union_all(self, other):
        self._unions.append((UnionType.all, other))

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
    def _list_aliases(field_set):
        return [field.alias or field.get_sql(with_quotes=False)
                for field in field_set]

    def _select_field_str(self, term):
        if term == '*':
            self._select_star = True
            self._selects = [Star()]
            return

        self._select_field(Field(term, table=self._from))

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

    def select_aliases(self):
        """
        Gets a list of aliases for the columns in this query's SELECT clause.  If a field does not have an alias, the
        field name is returned instead.

        :return:
            A list[str] of aliases.
        """
        return self._list_aliases(self._selects)

    def groupby_aliases(self):
        """
        Gets a list of aliases for the columns in this query's GROUP BY clause.  If a field does not have an alias, the
        field name is returned instead.

        :return:
            A list[str] of aliases.
        """
        return self._list_aliases(self._groupbys)

    def do_join(self, item, criterion, how):
        self._validate_join(criterion, item)

        if isinstance(item, QueryBuilder):
            self._tag_subquery(item)

        if (isinstance(item, Table) and isinstance(self._from, Table) and
                    item.alias is None and item.table_name == self._from.table_name):
            # On the odd chance that we join the same table as the FROM table and don't set an alias
            item.alias = item.table_name + '2'

        self._joins.append(Join(item, criterion, how))

    def _validate_term(self, term):
        for field in term.fields():
            if (field.table is not None
                and field.table != self._from
                and field.table not in [join.item for join in self._joins]):
                raise JoinException('Table [%s] missing from query.  '
                                    'Table must be first joined before any of '
                                    'its fields can be used' % field.table)

    def _validate_join(self, criterion, item):
        criterion_tables = set([f.table for f in criterion.fields()])
        available_tables = ({self._from} | {join.item for join in self._joins} | {item})
        missing_tables = criterion_tables - available_tables
        if missing_tables:
            raise JoinException('Invalid join criterion. One field is required from the joined item and '
                                'another from the selected table or an existing join.  Found [{tables}]'.format(
                tables=', '.join(map(str, missing_tables))
            ))

    def _tag_subquery(self, subquery):
        subquery.alias = 'sq%d' % self._subquery_count
        self._subquery_count += 1

    def __str__(self):
        return self.get_sql(with_unions=True)

    def __eq__(self, other):
        if not isinstance(other, QueryBuilder):
            return False

        if not self.alias == other.alias:
            return False

        if not self._from == other._from:
            return False

        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.alias) + hash(self._from)

    def get_sql(self, with_alias=False, subquery=False, with_unions=False, **kwargs):
        if not (self._selects or self._insert_table):
            return ''
        if self._insert_table and not (self._selects or self._values):
            return ''

        kwargs = {'with_namespace': bool(self._joins)}

        if not self._select_into and self._insert_table:
            querystring = self._insert_sql()

            if self._columns:
                querystring += self._columns_sql()

            if self._values:
                return querystring + self._values_sql()
            else:
                querystring += ' ' + self._select_sql(**kwargs)

        else:
            querystring = self._select_sql(**kwargs)

            if self._insert_table:
                querystring += self._into_sql()

        if self._from:
            querystring += self._from_sql()

        if self._joins:
            for join_item in self._joins:
                if join_item.how.value:
                    querystring += self._jointype_sql(join_item)
                querystring += self._join_sql(join_item, **kwargs)

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

        if self._offset:
            querystring += self._offset_sql()

        if self._limit:
            querystring += self._limit_sql()

        if subquery:
            querystring = '({query})'.format(
                query=querystring,
            )

        if with_alias:
            return self._queryalias_sql(querystring)

        if with_unions:
            querystring += self._union_sql(querystring)

        return querystring

    def _select_sql(self, **kwargs):
        return 'SELECT {distinct}{select}'.format(
            distinct='distinct ' if self._distinct else '',
            select=','.join(term.get_sql(with_quotes=True, with_alias=True, **kwargs)
                            for term in self._selects),
        )

    def _insert_sql(self):
        return 'INSERT {ignore}INTO {table}'.format(
            table=self._insert_table,
            ignore='IGNORE ' if self._ignore else ''
        )

    def _columns_sql(self):
        return ' ({columns})'.format(
            columns=','.join(term.get_sql(with_quotes=True, with_alias=True)
                             for term in self._columns)
        )

    def _values_sql(self):
        return ' VALUES ({values})'.format(
            values='),('.join(','.join(term.get_sql(with_quotes=True, with_alias=True)
                                       for term in row)
                              for row in self._values)

        )

    def _into_sql(self):
        return ' INTO {table}'.format(
            table=self._insert_table.get_sql(with_quotes=True, with_alias=False),
        )

    def _from_sql(self):
        return ' FROM {selectable}'.format(
            selectable=self._from.get_sql(with_quotes=True, subquery=True, with_alias=bool(self._joins)),
        )

    def _jointype_sql(self, join_item):
        return ' {type}'.format(type=join_item.how.value)

    def _join_sql(self, join, **kwargs):
        return ' JOIN {table} ON {criterion}'.format(
            table=join.item.get_sql(with_quotes=True, subquery=True, with_alias=True),
            criterion=join.criteria.get_sql(with_quotes=True, **kwargs),
        )

    def _where_sql(self, **kwargs):
        return ' WHERE {where}'.format(where=self._wheres.get_sql(with_quotes=True, subquery=True, **kwargs))

    def _group_sql(self, **kwargs):
        return ' GROUP BY {groupby}'.format(
            groupby=','.join(term.get_sql(with_quotes=True, **kwargs)
                             for term in self._groupbys)
        )

    def _rollup_sql(self):
        return ' WITH ROLLUP'

    def _having_sql(self, **kwargs):
        return ' HAVING {having}'.format(having=self._havings.get_sql(with_quotes=True, **kwargs))

    def _orderby_sql(self, **kwargs):
        return ' ORDER BY {orderby}'.format(
            orderby=','.join(
                '{field} {orient}'.format(
                    field=field.get_sql(with_quotes=True),
                    orient=orient.value,
                ) if orient is not None else
                field.get_sql(with_quotes=True, **kwargs)
                for field, orient in self._orderbys
            )
        )

    def _queryalias_sql(self, querystring):
        return '{query} \"{alias}\"'.format(
            query=querystring,
            alias=self.alias or self.table_name,
        )

    def _union_sql(self, querystring):
        unionstring = ''
        if self._unions:
            for (union_type, other) in self._unions:
                if len(self._selects) != len(other._selects):
                    raise UnionException("Queries must have an equal number of select statements in a union."
                                         "\n\nMain Query:\n{query1}"
                                         "\n\nUnion Query:\n{query2}".format(query1=querystring,
                                                                             query2=other.get_sql()))

                unionstring += ' UNION{type} {query}'.format(
                    type=union_type.value,
                    query=other.get_sql()
                )
        return unionstring

    def _offset_sql(self):
        return " OFFSET {offset}".format(offset=self._offset)

    def _limit_sql(self):
        return " LIMIT {limit}".format(limit=self._limit)


class Joiner(object):
    def __init__(self, query, item, how, type_label):
        self.query = query
        self.item = item
        self.how = how
        self.type_label = type_label

    def on(self, criterion):
        if criterion is None:
            raise JoinException("Parameter 'on' is required when joining a "
                                "{type} but was not supplied.".format(type=self.type_label))

        self.query.do_join(self.item, criterion, self.how)
        return self.query


class Join(object):
    def __init__(self, item, criteria, how):
        self.item = item
        self.criteria = criteria
        self.how = how
