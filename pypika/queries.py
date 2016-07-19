# coding: utf8
from collections import OrderedDict

from pypika.enums import JoinType, UnionType
from pypika.utils import JoinException, UnionException
from pypika.utils import immutable
from .terms import Field, Star, Term, Function, ArithmeticExpression

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


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


def make_tables(*names, **kwargs):
    return [Table(name, schema=kwargs.get('schema')) for name in names]


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
            return Query(table)

        return Query(Table(table))

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
        return Query().select(*fields)

    @staticmethod
    def _list_aliases(field_set):
        return [field.alias or field.get_sql(with_quotes=False)
                for field in field_set]

    def __init__(self, table=None):
        super(Query, self).__init__(None)

        self.table = table
        self.tables = OrderedDict()

        self.select = self._instance_select
        self.from_ = self._instance_from_

        if table is not None:
            self.tables[table.item_id] = table

        self._selects = []
        self._distinct = False

        self._wheres = None
        self._groupbys = []
        self._havings = None
        self._orderbys = []
        self._joins = []
        self._unions = []

        self._select_star = False
        self._select_star_tables = set()

    @immutable
    def _instance_from_(self, table):
        if self.table is not None:
            raise AttributeError("'TableQuery' object has no attribute 'from_'")

        if isinstance(table, Table):
            self.table = table
        else:
            self.table = Table(table)

        return self

    @immutable
    def _instance_select(self, *terms):
        for term in terms:
            if isinstance(term, Field):
                self._select_field(self._replace_table_ref(term))
            elif isinstance(term, str):
                self._select_field_str(term)
            elif isinstance(term, (Function, ArithmeticExpression)):
                self._select_other(self._replace_table_ref(term))
            else:
                self._select_other(self._wrap(term))

        return self

    def _select_field_str(self, term):
        if term == '*':
            self._select_star = True
            self._selects = [Star()]
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
            self._selects = [select
                             for select in self._selects
                             if not hasattr(select, 'table') or term.table != select.table]
            self._select_star_tables.add(term.table)

        self._selects.append(term)

    def _select_other(self, function):
        self._selects.append(function)

    @immutable
    def distinct(self):
        self._distinct = True
        return self

    @immutable
    def where(self, criterion):
        self._replace_table_ref(criterion)

        if self._wheres:
            self._wheres &= criterion
        else:
            self._wheres = criterion

        return self

    def having(self, criteria):
        for field in criteria.fields():
            self._replace_table_ref(field)

        if self._havings:
            self._havings &= criteria
        else:
            self._havings = criteria

        return self

    @immutable
    def groupby(self, *fields):
        for field in fields:
            if isinstance(field, str):
                field = Field(field, table=self.table)
            self._groupbys.append(self._replace_table_ref(field))

        return self

    @immutable
    def orderby(self, *fields, **kwargs):
        for field in fields:
            if isinstance(field, str):
                field = Field(field, table=self.table)
            else:
                field = self._replace_table_ref(self._wrap(field))

            self._orderbys.append((field, kwargs.get('order')))

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
        self._unions.append((UnionType.distinct, other))
        return self

    @immutable
    def union_all(self, other):
        self._unions.append((UnionType.all, other))
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
        self.tables[item.item_id] = item
        self._joins.append(Join(item.item_id, criterion, how))

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

    def __str__(self):
        querystring = self.get_sql()

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

        return querystring + unionstring

    def get_sql(self, with_alias=False, subquery=False, **kwargs):
        if not self._selects:
            return ''

        if self._joins:
            for i, table in enumerate(self.tables.values()):
                table.alias = table.alias or 't%d' % i

        querystring = 'SELECT {distinct}{select}'.format(
            distinct='distinct ' if self._distinct else '',
            select=','.join(term.get_sql(with_quotes=True, with_alias=True)
                            for term in self._selects),
        )

        if self.table:
            querystring += ' FROM {table}'.format(
                table=self.table.get_sql(with_quotes=True),
            )

        if self._joins:
            for join_item in self._joins:
                if join_item.how.value:
                    querystring += ' {type}'.format(type=join_item.how.value)

                querystring += ' JOIN {table} ON {criterion}'.format(
                    table=self.tables[join_item.table_id].get_sql(with_quotes=True, with_alias=True, subquery=True),
                    criterion=join_item.criteria.get_sql(with_quotes=True),
                )

        if self._wheres:
            querystring += ' WHERE {where}'.format(where=self._wheres.get_sql(with_quotes=True, subquery=True))

        if self._groupbys:
            querystring += ' GROUP BY {groupby}'.format(
                groupby=','.join(term.get_sql(with_quotes=True)
                                 for term in self._groupbys)
            )

        if self._havings:
            querystring += ' HAVING {having}'.format(having=self._havings.get_sql(with_quotes=True))

        if self._orderbys:
            querystring += ' ORDER BY {orderby}'.format(
                orderby=','.join(
                    '{field} {orient}'.format(
                        field=field.get_sql(with_quotes=True),
                        orient=orient.value,
                    ) if orient is not None else
                    field.get_sql(with_quotes=True)
                    for field, orient in self._orderbys
                )
            )

        if subquery:
            querystring = '({query})'.format(
                query=querystring,
            )

        if with_alias:
            return '{query} \"{alias}\"'.format(
                query=querystring,
                alias=self.alias,
            )

        return querystring


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
