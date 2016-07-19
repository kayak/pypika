# coding: utf8
from collections import OrderedDict

from pypika.enums import JoinType, UnionType
from pypika.utils import JoinException, UnionException
from pypika.utils import immutable
from .terms import Field, Star, Term

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"
__version__ = "0.0.1"


class Selectable(object):
    def __init__(self, alias):
        self.item_id = id(self)
        self.alias = alias
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
    def __init__(self, name):
        super(Table, self).__init__(None)
        self.table_name = name

    def __str__(self):
        return self.get_sql()

    def get_sql(self, **kwargs):
        # FIXME escape
        if self.alias:
            return "`{name}` `{alias}`".format(
                name=self.table_name,
                alias=self.alias
            )
        return "`{name}`".format(
            name=self.table_name
        )

    def __eq__(self, other):
        return isinstance(other, Table) and self.table_name == other.table_name

    def __hash__(self):
        return self.item_id


def make_tables(*names):
    return [Table(name) for name in names]


class Query(Selectable, Term):
    """
    Query is the primary class and entry point in pypika. It is used to build queries iteratively using the builder design
    pattern.

    This class is immutable.
    """

    @staticmethod
    def from_(table):
        """
        Primary entry point for building queries that select from a table.

        This function creates a new instance of a TableQuery for the parameter table.

        :param table:
            Type: Table or str

            An instance of a Table object or a string.

        :returns TableQuery
        """
        if isinstance(table, Table):
            return TableQuery(table, [])
        return TableQuery(Table(table), [])

    @staticmethod
    def select(*fields):
        """
        Secondary entry point for building queries that select without a table.  It is also possible to convert a Query
        to a TableQuery after calling this function by calling the from_ function.  This function is perhaps useful for
        testing SQL syntax.

        :param table:
            Type: Table or str

            An instance of a Table object or a string.

        :returns TableQuery
        """
        return Query(fields)

    @staticmethod
    def _list_aliases(field_set):
        return [field.alias or field.get_sql(with_quotes=False)
                for field in field_set]

    def __init__(self, select):
        super(Query, self).__init__(None)

        self.select_parts = [self._wrap(field) for field in select]
        self._distinct = False

        self._select_star = False
        self._select_star_tables = set()

        # After instantiating, replace these functions with instance versions so the names can be reused.
        self.from_ = self._instance_from_
        self.select = self._instance_select

        self.table = None
        self.tables = OrderedDict()

        self.where_parts = None
        self.groupby_parts = None
        self.having_parts = None
        self.orderby_parts = None

        self.join_parts = None
        self.union_parts = None

    @immutable
    def _instance_from_(self, table):
        """
        Add a table to the query.  This is an alternative path for when the static select function is called first which
        allows a table to be added to the query second.


        :param table:
        :return:
        """
        if isinstance(table, Table):
            return TableQuery(table, self.select_parts)
        return TableQuery(Table(table), self.select_parts)

    @immutable
    def _instance_select(self, *fields):
        for field in fields:
            self.select_parts.append(self._wrap(field))

        return self

    def __str__(self):
        querystring = self.get_sql()

        unionstring = ''
        if self.union_parts:
            for (union_type, other) in self.union_parts:
                if len(self.select_parts) != len(other.select_parts):
                    raise UnionException("Queries must have an equal number of select statements in a union."
                                         "\n\nMain Query:\n{query1}"
                                         "\n\nUnion Query:\n{query2}".format(query1=querystring,
                                                                             query2=other.get_sql()))

                unionstring += ' UNION{type} {query}'.format(
                    type=union_type.value,
                    query=other.get_sql()
                )

        return querystring + unionstring

    def get_sql(self, with_alias=False, subquery=False, **kwargs):
        if not self.select_parts:
            return ''

        if self.join_parts:
            for i, table in enumerate(self.tables.values()):
                table.alias = table.alias or 't%d' % i

        querystring = 'SELECT {distinct}{select}'.format(
            distinct='distinct ' if self._distinct else '',
            select=','.join(term.get_sql(with_quotes=True, with_alias=True)
                            for term in self.select_parts),
        )

        if self.table:
            querystring += ' FROM {table}'.format(
                table=self.table.get_sql(with_quotes=True),
            )

        if self.join_parts:
            for join_item in self.join_parts:
                if join_item.how.value:
                    querystring += ' {type}'.format(type=join_item.how.value)

                querystring += ' JOIN {table} ON {criterion}'.format(
                    table=self.tables[join_item.table_id].get_sql(with_quotes=True, with_alias=True, subquery=True),
                    criterion=join_item.criteria.get_sql(with_quotes=True),
                )

        if self.where_parts:
            querystring += ' WHERE {where}'.format(where=self.where_parts.get_sql(with_quotes=True, subquery=True))

        if self.groupby_parts:
            querystring += ' GROUP BY {groupby}'.format(
                groupby=','.join(term.get_sql(with_quotes=True)
                                 for term in self.groupby_parts)
            )

        if self.having_parts:
            querystring += ' HAVING {having}'.format(having=self.having_parts.get_sql(with_quotes=True))

        if self.orderby_parts:
            querystring += ' ORDER BY {orderby}'.format(
                orderby=','.join(
                    '{field} {orient}'.format(
                        field=field.get_sql(with_quotes=True),
                        orient=orient.value,
                    ) if orient is not None else
                    field.get_sql(with_quotes=True)
                    for field, orient in self.orderby_parts
                )
            )

        if subquery:
            querystring = '({query})'.format(
                query=querystring,
            )

        if with_alias:
            return '{query} `{alias}`'.format(
                query=querystring,
                alias=self.alias,
            )

        return querystring


class TableQuery(Query):
    def __init__(self, table, select):
        super(TableQuery, self).__init__(select)

        self.table = table
        self.tables = OrderedDict({table.item_id: table})

        self.where_parts = None
        self.groupby_parts = []
        self.having_parts = None
        self.orderby_parts = []

        self.join_parts = []
        self.union_parts = []

        self._nested = False

    @immutable
    def _instance_from_(self, table):
        raise AttributeError("'TableQuery' object has no attribute 'from_'")

    @immutable
    def _instance_select(self, *terms):
        for term in terms:
            if isinstance(term, str):
                self._select_field_str(term)
            elif isinstance(term, Field):
                self._select_field(self._replace_table_ref(term))
            else:
                self._select_function(self._replace_table_ref(term))

        return self

    def _select_field_str(self, term):
        if term == '*':
            self._select_star = True
            self.select_parts = [Star()]
            return

        self._select_field(Field(term, table=self.table))

    def _select_field(self, term):
        if self._select_star:
            # Do not add select terms after a star is selected
            return

        if term.table in self._select_star_tables:
            # Do not add select terms for table after a table star is selected
            return

        if isinstance(term, Star):
            self.select_parts = [select
                                 for select in self.select_parts
                                 if not hasattr(select, 'table') or term.table != select.table]
            self._select_star_tables.add(term.table)

        self.select_parts.append(term)

    def _select_function(self, function):
        self.select_parts.append(function)

    @immutable
    def distinct(self):
        self._distinct = True
        return self

    @immutable
    def where(self, criterion):
        self._replace_table_ref(criterion)

        if self.where_parts:
            self.where_parts &= criterion
        else:
            self.where_parts = criterion

        return self

    def having(self, criteria):
        for field in criteria.fields():
            self._replace_table_ref(field)

        if self.having_parts:
            self.having_parts &= criteria
        else:
            self.having_parts = criteria

        return self

    @immutable
    def groupby(self, *fields):
        for field in fields:
            if isinstance(field, str):
                field = Field(field, table=self.table)
            self.groupby_parts.append(self._replace_table_ref(field))

        return self

    @immutable
    def orderby(self, *fields, **kwargs):
        for field in fields:
            if isinstance(field, str):
                field = Field(field, table=self.table)
            else:
                field = self._replace_table_ref(self._wrap(field))

            self.orderby_parts.append((field, kwargs.get('order')))

        return self

    @immutable
    def join(self, item, how=JoinType.left):
        if isinstance(item, Table):
            return TableJoiner(self, item, how)

        elif isinstance(item, Query):
            return SubqueryJoiner(self, item, how)

        raise ValueError("Cannot join on type '%s'" % type(item))

    @immutable
    def union(self, other):
        self.union_parts.append((UnionType.distinct, other))
        return self

    @immutable
    def union_all(self, other):
        self.union_parts.append((UnionType.all, other))
        return self

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
        return self._list_aliases(self.select_parts)

    def groupby_aliases(self):
        """
        Gets a list of aliases for the columns in this query's GROUP BY clause.  If a field does not have an alias, the
        field name is returned instead.

        :return:
            A list[str] of aliases.
        """
        return self._list_aliases(self.groupby_parts)

    def do_join(self, item, criterion, how):
        self.tables[item.item_id] = item
        self.join_parts.append(Join(item.item_id, criterion, how))

        for field in criterion.fields():
            self._replace_table_ref(field)

    def _replace_table_ref(self, term):
        for field in term.fields():
            if field.table is None:
                field.table = self.table
                continue

            if field.table.item_id not in self.tables:
                raise JoinException('Table [%s] missing from query.  '
                                    'Table must be first joined before any of '
                                    'its fields can be used' % field.table)

            field.table = self.tables[field.table.item_id]
        return term

    def __add__(self, other):
        return self.union(other)

    def __mul__(self, other):
        return self.union_all(other)


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

        self.subquery._nested = True
        self.query.do_join(self.subquery, criterion, self.how)
        return self.query


class Join(object):
    def __init__(self, table_id, criteria, how):
        self.table_id = table_id
        self.criteria = criteria
        self.how = how
