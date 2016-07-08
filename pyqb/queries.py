# coding: utf8
from collections import OrderedDict

from pyqb.enums import Equality, JoinType
from pyqb.utils import JoinException
from pyqb.utils import immutable
from .terms import Field, Star, Term

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"
__version__ = "0.0.1"


class Selectable(object):
    def __init__(self, id, alias):
        self._id = id
        self._alias = alias

    def __getattr__(self, name):
        # This prevents Fields being when deepcopy functions are called
        if name in ['__deepcopy__', '__getstate__', '__setstate__', '__getnewargs__']:
            raise AttributeError("'Table' object has no attribute '%s'" % name)

        return Field(name, table=self)

    @property
    def star(self):
        return Star(table=self)

    def __hash__(self):
        return self._id


class Table(Selectable):
    def __init__(self, name):
        super(Table, self).__init__(id(self), None)
        self._name = name

    def __str__(self):
        # FIXME escape
        if self._alias:
            return "{name} {alias}".format(
                name=self._name,
                alias=self._alias
            )
        return self._name

    def __eq__(self, other):
        return isinstance(other, Table) and self._name == other._name

    def __hash__(self):
        return self._id


def make_tables(*names):
    return [Table(name) for name in names]


class Query(Selectable, Term):
    """
    Query is the primary class and entry point in pyqb. It is used to build queries iteratively using the builder design
    pattern.

    This class is immutable.
    """

    @staticmethod
    def from_(table):
        """

        :param table:
            Type: Table or str

            An instance of a Table object or a string.
        """
        if isinstance(table, Table):
            return TableQuery(table, [])
        return TableQuery(Table(table), [])

    def select(self, *fields):
        return self

    @staticmethod
    def select(*fields):
        return Query(fields)

    def __init__(self, select):
        super(Query, self).__init__(id(self), None)

        self._select = select
        self._distinct = False

        self._select_star = False
        self._select_star_tables = set()

    def __str__(self):
        return 'SELECT {distinct}{select}'.format(
            distinct='distinct ' if self._distinct else '',
            select=','.join(self._render_alias(term)
                            for term in self._select),
        )

    @staticmethod
    def _render_alias(term):
        alias = getattr(term, '_alias', None)
        if alias is not None:
            return '{field} {alias}'.format(field=str(term), alias=term._alias)

        return str(term)


class TableQuery(Query):
    def __init__(self, table, select):
        super(TableQuery, self).__init__(select)

        self._table = table
        self._tables = OrderedDict({table._id: table})

        self._where = None
        self._groupby = []
        self._having = None
        self._orderby = []

        self._join = []

        self._nested = False

    @immutable
    def select(self, *fields):
        for field in fields:
            if isinstance(field, str):
                self._select_field_str(field)
            elif isinstance(field, Field):
                self._select_field(self._replace_table_ref(field))
            else:
                self._select_function(self._replace_table_ref(field))

        return self

    def _select_field_str(self, field):
        if field == '*':
            self._select_star = True
            self._select = [Star()]
            return

        self._select_field(Field(field, table=self._table))

    def _select_field(self, field):
        if self._select_star:
            # Do not add select terms after a star is selected
            return

        if field.table in self._select_star_tables:
            # Do not add select terms for table after a table star is selected
            return

        if isinstance(field, Star):
            self._select = [select
                            for select in self._select
                            if select.table != field.table]
            self._select_star_tables.add(field.table)

        self._select.append(field)

    def _select_function(self, function):
        self._select.append(function)

    @immutable
    def distinct(self):
        self._distinct = True
        return self

    @immutable
    def where(self, criterion):
        self._replace_table_ref(criterion)

        if self._where:
            self._where &= criterion
        else:
            self._where = criterion

        return self

    def having(self, criteria):
        for field in criteria.fields():
            self._replace_table_ref(field)

        if self._having:
            self._having &= criteria
        else:
            self._having = criteria

        return self

    @immutable
    def groupby(self, *fields):
        for field in fields:
            if type(field) is str:
                field = Field(field, table=self._table)
            self._groupby.append(self._replace_table_ref(field))

        return self

    @immutable
    def orderby(self, *fields, **kwargs):
        for field in fields:
            if type(field) is not Field:
                field = Field(field, table=self.table)
            self._orderby.append((field, kwargs.get('order')))

        return self

    @immutable
    def join(self, item, how=JoinType.left):
        if isinstance(item, Table):
            return TableJoiner(self, item, how)

        elif isinstance(item, Query):
            return SubqueryJoiner(self, item, how)

        raise ValueError("Cannot join on type '%s'" % type(item))

    def fields(self):
        # Don't return anything here. Subqueries have their own fields.
        return []

    def _replace_table_ref(self, item):
        for field in item.fields():
            if field.table is None:
                field.table = self._table
                continue

            if field.table._id not in self._tables:
                # TODO write better exception message
                raise JoinException('Table [%s] missing from query.  '
                                    'Table must be first joined before any of '
                                    'its fields can be used' % field.table)

            field.table = self._tables[field.table._id]
        return item

    def __str__(self):
        if self._join:
            for i, table in enumerate(self._tables.values()):
                table._alias = table._alias or 't%d' % i

        querystring = 'SELECT {distinct}{select} FROM {table}'.format(
            table=str(self._table),
            distinct='distinct ' if self._distinct else '',
            select=','.join(self._render_alias(term)
                            for term in self._select),
        )

        if self._join:
            for join_item in self._join:
                if join_item.how.value:
                    querystring += ' {type}'.format(type=join_item.how.value)

                querystring += ' JOIN {table} ON {criterion}'.format(
                    table=str(self._tables[join_item.id]),
                    criterion=str(join_item.criteria),
                )

        if self._where:
            querystring += ' WHERE {where}'.format(where=self._where)

        if self._groupby:
            querystring += ' GROUP BY {groupby}'.format(
                groupby=','.join(map(str, self._groupby))
            )

        if self._having:
            querystring += ' HAVING {having}'.format(having=self._having)

        if self._orderby:
            querystring += ' ORDER BY {orderby}'.format(
                orderby=','.join(
                    '{field} {orient}'.format(
                        field=str(field),
                        orient=orient.value,
                    ) if orient is not None else str(field)
                    for field, orient in self._orderby
                )
            )

        if self._nested:
            querystring = '({})'.format(querystring)

        if self._alias is not None:
            return '{query} {alias}'.format(
                query=querystring,
                alias=self._alias
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

        if Equality.eq != criterion.comparator or 2 > len(criterion.fields()):
            # TODO write write a better message
            raise JoinException('Equality Criterion of two fields is required for joining a table.  '
                                '[%s] is invalid.' % str(criterion))

        # FIXME need to be able to use joined tables too
        mismatched_tables = {self.table, self.query._table} ^ {field.table for field in criterion.fields()}
        if mismatched_tables:
            # TODO write write a better message
            raise JoinException('Join required a field from each query.  '
                                '[%s] is invalid.' % str(criterion))

        self.query._tables[self.table._id] = self.table
        self.query._join.append(Join(self.table._id, criterion, self.how))

        for field in criterion.fields():
            self.query._replace_table_ref(field)

        return self.query


class SubqueryJoiner(Joiner):
    def __init__(self, query, subquery, how):
        super(SubqueryJoiner, self).__init__(query, how)
        self.subquery = subquery

    def on(self, criterion):
        if criterion is None:
            raise JoinException("Parameter 'on' is required when joining a subquery but was not supplied.")

        if Equality.eq != criterion.comparator or 2 > len(criterion.fields()):
            # TODO write write a better message
            raise JoinException('Equality Criterion of two fields is required for joining a subquery.  '
                                '[%s] is invalid.' % str(criterion))

        # TODO validate that one field comes from a table in the current query

        # TODO validate that one field is included in subquey selects as alias or otherwise

        self.subquery._nested = True
        self.query._tables[self.subquery._id] = self.subquery
        self.query._join.append(Join(self.subquery._id, criterion, self.how))

        for field in criterion.fields():
            self.query._replace_table_ref(field)

        return self.query


class Join(object):
    def __init__(self, id, criteria, how):
        self.id = id
        self.criteria = criteria
        self.how = how
