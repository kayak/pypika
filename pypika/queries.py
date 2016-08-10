# coding: utf8
from collections import OrderedDict

from pypika.enums import JoinType, UnionType
from pypika.utils import JoinException, UnionException, RollupException
from pypika.utils import builder
from .terms import Field, Star, Term, Function, ArithmeticExpression, Rollup

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class Selectable(object):
    def __init__(self, alias):
        self.item_id = id(self)
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

    def __hash__(self):
        return self.item_id


class Table(Selectable):
    def __init__(self, name, schema=None):
        super(Table, self).__init__(None)
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

    def __eq__(self, other):
        return isinstance(other, Table) and self.table_name == other.table_name

    def __hash__(self):
        return self.item_id

    def __str__(self):
        return self.get_sql()


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

        self._selectables = OrderedDict()

        self._from = None
        self._insert_table = None

        self._selects = []
        self._columns = []
        self._values = []
        self._distinct = False

        self._wheres = None
        self._groupbys = []
        self._havings = None
        self._orderbys = []
        self._joins = []
        self._unions = []

        self._select_star = False
        self._select_star_tables = set()
        self._mysql_rollup = False
        self._select_into = False

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
        self._selectables[self._from.item_id] = self._from

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
                self._select_field(self._replace_table_ref(term))
            elif isinstance(term, str):
                self._select_field_str(term)
            elif isinstance(term, (Function, ArithmeticExpression)):
                self._select_other(self._replace_table_ref(term))
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
    def where(self, criterion):
        self._replace_table_ref(criterion)

        if self._wheres:
            self._wheres &= criterion
        else:
            self._wheres = criterion

    @builder
    def having(self, criteria):
        for field in criteria.fields():
            self._replace_table_ref(field)

        if self._havings:
            self._havings &= criteria
        else:
            self._havings = criteria

    @builder
    def groupby(self, *fields):
        for field in fields:
            if isinstance(field, str):
                field = Field(field, table=self._from)
            self._groupbys.append(self._replace_table_ref(field))

    @builder
    def rollup(self, *fields, **kwargs):
        for_mysql = 'mysql' == kwargs.get('vendor')

        if self._mysql_rollup:
            raise AttributeError("'Query' object has no attribute '%s'" % 'rollup')

        if for_mysql:
            if not fields and not self._groupbys:
                raise RollupException('At least one group is required. Call Query.groupby(term) or pass'
                                      'as parameter to rollup.')

            self._mysql_rollup = True
            self._groupbys += fields

        else:
            self._groupbys.append(Rollup(*fields))

    @builder
    def orderby(self, *fields, **kwargs):
        for field in fields:
            if isinstance(field, str):
                field = Field(field, table=self._from)
            else:
                field = self._replace_table_ref(self._wrap(field))

            self._orderbys.append((field, kwargs.get('order')))

    @builder
    def join(self, item, how=JoinType.left):
        if isinstance(item, Table):
            return TableJoiner(self, item, how)

        elif isinstance(item, QueryBuilder):
            return SubqueryJoiner(self, item, how)

        raise ValueError("Cannot join on type '%s'" % type(item))

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
        self._selectables[item.item_id] = item
        self._joins.append(Join(item.item_id, criterion, how))

        for field in criterion.fields():
            self._replace_table_ref(field)

    def _replace_table_ref(self, term):
        for field in term.fields():
            if field.table is None:
                field.table = self._from
                continue

            if field.table.item_id not in self._selectables:
                raise JoinException('Table [%s] missing from query.  '
                                    'Table must be first joined before any of '
                                    'its fields can be used' % field.table)

            field.table = self._selectables[field.table.item_id]
        return term

    def __str__(self):
        return self.get_sql(with_unions=True)

    def get_sql(self, with_alias=False, subquery=False, with_unions=False, **kwargs):
        if not (self._selects or self._insert_table):
            return ''
        if self._insert_table and not (self._selects or self._values):
            return ''

        if self._joins:
            for i, table in enumerate(self._selectables.values()):
                table.alias = table.alias or 't%d' % i

        if not self._select_into and self._insert_table:
            querystring = self._insert_sql()

            if self._columns:
                querystring += self._columns_sql()

            if self._values:
                return querystring + self._values_sql()
            else:
                querystring += ' ' + self._select_sql()

        else:
            querystring = self._select_sql()

            if self._insert_table:
                querystring += self._into_sql()

        if self._from:
            querystring += self._from_sql()

        if self._joins:
            for join_item in self._joins:
                if join_item.how.value:
                    querystring += self._jointype_sql(join_item)
                querystring += self._join_sql(join_item)

        if self._wheres:
            querystring += self._where_sql()

        if self._groupbys:
            querystring += self._group_sql()
            if self._mysql_rollup:
                querystring += self._rollup_sql()

        if self._havings:
            querystring += self._having_sql()

        if self._orderbys:
            querystring += self._orderby_sql()

        if subquery:
            querystring = '({query})'.format(
                query=querystring,
            )

        if with_alias:
            return self._queryalias_sql(querystring)

        if with_unions:
            querystring += self._union_sql(querystring)

        return querystring

    def _select_sql(self):
        return 'SELECT {distinct}{select}'.format(
            distinct='distinct ' if self._distinct else '',
            select=','.join(term.get_sql(with_quotes=True, with_alias=True)
                            for term in self._selects),
        )

    def _insert_sql(self):
        return 'INSERT INTO {table}'.format(
            table=self._insert_table
        )

    def _columns_sql(self):
        return ' ({columns})'.format(
            columns=','.join(term.get_sql(with_quotes=True, with_alias=True)
                             for term in self._columns)
        )

    def _values_sql(self):
        return ' VALUES ({values})'.format(
            values='),('.join(','.join
                              (term.get_sql(with_quotes=True, with_alias=True)
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

    def _join_sql(self, join_item):
        return ' JOIN {table} ON {criterion}'.format(
            table=self._selectables[join_item.table_id].get_sql(with_quotes=True, subquery=True, with_alias=True),
            criterion=join_item.criteria.get_sql(with_quotes=True),
        )

    def _where_sql(self):
        return ' WHERE {where}'.format(where=self._wheres.get_sql(with_quotes=True, subquery=True))

    def _group_sql(self):
        return ' GROUP BY {groupby}'.format(
            groupby=','.join(term.get_sql(with_quotes=True)
                             for term in self._groupbys)
        )

    def _rollup_sql(self):
        return ' WITH ROLLUP'

    def _having_sql(self):
        return ' HAVING {having}'.format(having=self._havings.get_sql(with_quotes=True))

    def _orderby_sql(self):
        return ' ORDER BY {orderby}'.format(
            orderby=','.join(
                '{field} {orient}'.format(
                    field=field.get_sql(with_quotes=True),
                    orient=orient.value,
                ) if orient is not None else
                field.get_sql(with_quotes=True)
                for field, orient in self._orderbys
            )
        )

    def _queryalias_sql(self, querystring):
        return '{query} \"{alias}\"'.format(
            query=querystring,
            alias=self.alias,
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


class Joiner(object):
    def __init__(self, query, how):
        self.query = query
        self.how = how

    def on(self, criterion):
        raise NotImplementedError()


class TableJoiner(Joiner):
    def __init__(self, query, table, how):
        super(TableJoiner, self).__init__(query, how)
        self.table = table

    def on(self, criterion):
        if criterion is None:
            raise JoinException("Parameter 'on' is required when joining a table but was not supplied.")

        self.query.do_join(self.table, criterion, self.how)
        return self.query


class SubqueryJoiner(Joiner):
    def __init__(self, query, subquery, how):
        super(SubqueryJoiner, self).__init__(query, how)
        self.subquery = subquery

    def on(self, criterion):
        if criterion is None:
            raise JoinException("Parameter 'on' is required when joining a subquery but was not supplied.")

        self.query.do_join(self.subquery, criterion, self.how)
        return self.query


class Join(object):
    def __init__(self, table_id, criteria, how):
        self.table_id = table_id
        self.criteria = criteria
        self.how = how
