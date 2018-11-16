# coding: utf-8
import inspect
import re
from datetime import date

from aenum import Enum

from pypika.enums import (
    Arithmetic,
    Boolean,
    Dialects,
    Equality,
    Matching,
)
from pypika.utils import (
    CaseException,
    alias_sql,
    builder,
    ignore_copy,
    resolve_is_aggregate,
)

try:
  basestring
except NameError:
  basestring = str


__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class Term(object):
    is_aggregate = False

    def __init__(self, alias=None):
        self.alias = alias

    @builder
    def as_(self, alias):
        self.alias = alias

    @property
    def tables_(self):
        return set()

    def wrap_constant(self, val):
        """
        Used for wrapping raw inputs such as numbers in Criterions and Operator.

        For example, the expression F('abc')+1 stores the integer part in a ValueWrapper object.

        :param val:
            Any value.
        :return:
            Raw string, number, or decimal values will be returned in a ValueWrapper.  Fields and other parts of the
            querybuilder will be returned as inputted.

        """
        from .queries import QueryBuilder

        if isinstance(val, (Term, QueryBuilder, Interval)):
            return val
        if val is None:
            return NullValue()
        if isinstance(val, list):
            return Array(*val)
        if isinstance(val, tuple):
            return Tuple(*val)

        _ValueWrapper = getattr(self, '_wrapper_cls', ValueWrapper)
        return _ValueWrapper(val)

    def for_(self, table):
        """
        Replaces the tables of this term for the table parameter provided.  The base implementation returns self because not all terms have a table property.

        :param table:
            The table to replace with.
        :return:
            Self.
        """
        return self

    def fields(self):
        return [self]

    def eq(self, other):
        return self == other

    def isnull(self):
        return NullCriterion(self)

    def notnull(self):
        return self.isnull().negate()

    def gt(self, other):
        return self > other

    def gte(self, other):
        return self >= other

    def lt(self, other):
        return self < other

    def lte(self, other):
        return self <= other

    def ne(self, other):
        return self != other

    def like(self, expr):
        return BasicCriterion(Matching.like, self, self.wrap_constant(expr))

    def not_like(self, expr):
        return BasicCriterion(Matching.not_like, self, self.wrap_constant(expr))

    def ilike(self, expr):
        return BasicCriterion(Matching.ilike, self, self.wrap_constant(expr))

    def not_ilike(self, expr):
        return BasicCriterion(Matching.not_ilike, self, self.wrap_constant(expr))

    def regex(self, pattern):
        return BasicCriterion(Matching.regex, self, self.wrap_constant(pattern))

    def between(self, lower, upper):
        return BetweenCriterion(self, self.wrap_constant(lower), self.wrap_constant(upper))

    def isin(self, arg):
        if isinstance(arg, (list, tuple, set)):
            return ContainsCriterion(self, Tuple(*[self.wrap_constant(value) for value in arg]))
        return ContainsCriterion(self, arg)

    def notin(self, arg):
        return self.isin(arg).negate()

    def bin_regex(self, pattern):
        return BasicCriterion(Matching.bin_regex, self, self.wrap_constant(pattern))

    def negate(self):
        return Not(self)

    def __invert__(self):
        return Not(self)

    def __pos__(self):
        return self

    def __neg__(self):
        return Negative(self)

    def __add__(self, other):
        return ArithmeticExpression(Arithmetic.add, self, self.wrap_constant(other))

    def __sub__(self, other):
        return ArithmeticExpression(Arithmetic.sub, self, self.wrap_constant(other))

    def __mul__(self, other):
        return ArithmeticExpression(Arithmetic.mul, self, self.wrap_constant(other))

    def __div__(self, other):
        # Required for Python2
        return self.__truediv__(other)

    def __truediv__(self, other):
        return ArithmeticExpression(Arithmetic.div, self, self.wrap_constant(other))

    def __pow__(self, other):
        return Pow(self, other)

    def __mod__(self, other):
        return Mod(self, other)

    def __radd__(self, other):
        return ArithmeticExpression(Arithmetic.add, self.wrap_constant(other), self)

    def __rsub__(self, other):
        return ArithmeticExpression(Arithmetic.sub, self.wrap_constant(other), self)

    def __rmul__(self, other):
        return ArithmeticExpression(Arithmetic.mul, self.wrap_constant(other), self)

    def __rdiv__(self, other):
        # Required for Python2
        return self.__rtruediv__(other)

    def __rtruediv__(self, other):
        return ArithmeticExpression(Arithmetic.div, self.wrap_constant(other), self)

    def __eq__(self, other):
        return BasicCriterion(Equality.eq, self, self.wrap_constant(other))

    def __ne__(self, other):
        return BasicCriterion(Equality.ne, self, self.wrap_constant(other))

    def __gt__(self, other):
        return BasicCriterion(Equality.gt, self, self.wrap_constant(other))

    def __ge__(self, other):
        return BasicCriterion(Equality.gte, self, self.wrap_constant(other))

    def __lt__(self, other):
        return BasicCriterion(Equality.lt, self, self.wrap_constant(other))

    def __le__(self, other):
        return BasicCriterion(Equality.lte, self, self.wrap_constant(other))

    def __getitem__(self, item):
        if not isinstance(item, slice):
            raise TypeError("Field' object is not subscriptable")
        return self.between(item.start, item.stop)

    def __str__(self):
        return self.get_sql(quote_char='"')

    def __hash__(self):
        return hash(self.get_sql(with_alias=True))

    def get_sql(self):
        raise NotImplementedError()


class Negative(Term):
    def __init__(self, term):
        super(Negative, self).__init__()
        self.term = term

    def get_sql(self, **kwargs):
        return '-{term}'.format(term=self.term.get_sql(**kwargs))


class ValueWrapper(Term):
    is_aggregate = None

    def __init__(self, value, alias=None):
        super(ValueWrapper, self).__init__(alias)
        self.value = value

    def fields(self):
        return []

    def get_sql(self, quote_char=None, **kwargs):
        sql = self._get_value_sql(quote_char=quote_char, **kwargs)
        return alias_sql(sql, self.alias, quote_char)

    def _get_value_sql(self, quote_char=None, **kwargs):
        # FIXME escape values
        if isinstance(self.value, Term):
            return self.value.get_sql(quote_char=quote_char, **kwargs)
        if isinstance(self.value, Enum):
            return self.value.value
        if isinstance(self.value, date):
            return "'%s'" % self.value.isoformat()
        if isinstance(self.value, basestring):
            value = self.value.replace("'", "''")
            return "'%s'" % value
        if isinstance(self.value, bool):
            return str.lower(str(self.value))
        if self.value is None:
            return 'null'
        return str(self.value)


class Values(Term):
    def __init__(self, field,):
        super(Values, self).__init__(None)
        self.field = Field(field) if not isinstance(field, Field) else field

    def get_sql(self, quote_char=None, **kwargs):
        return 'VALUES({value})'.format(value=self.field.get_sql(quote_char=quote_char, **kwargs))


class NullValue(Term):
    def fields(self):
        return []

    def get_sql(self, quote_char=None, **kwargs):
        sql = 'NULL'
        if self.alias is None:
            return sql
        return alias_sql(sql, self.alias, quote_char)


class Criterion(Term):
    def __and__(self, other):
        return ComplexCriterion(Boolean.and_, self, other)

    def __or__(self, other):
        return ComplexCriterion(Boolean.or_, self, other)

    def __xor__(self, other):
        return ComplexCriterion(Boolean.xor_, self, other)

    def fields(self):
        raise NotImplementedError()

    def get_sql(self):
        raise NotImplementedError()


class Field(Criterion):
    def __init__(self, name, alias=None, table=None):
        super(Field, self).__init__(alias)
        self.name = name
        self.table = table

    def __and__(self, other):
        return ComplexCriterion(Boolean.and_, self, other)

    def __or__(self, other):
        return ComplexCriterion(Boolean.or_, self, other)

    def __xor__(self, other):
        return ComplexCriterion(Boolean.xor_, self, other)

    def fields(self):
        return [self]

    @property
    def tables_(self):
        return {self.table}

    @builder
    def for_(self, table):
        """
        Replaces the tables of this term for the table parameter provided.  Useful when reusing fields across queries.

        :param table:
            The table to replace with.
        :return:
            A copy of the field with it's table value replaced.
        """
        self.table = table

    def get_sql(self, with_alias=False, with_namespace=False, quote_char=None, **kwargs):
        # Need to add namespace if the table has an alias
        if self.table and (with_namespace or self.table.alias):
            field_sql = "{quote}{namespace}{quote}.{quote}{name}{quote}".format(
                namespace=self.table.alias or self.table._table_name,
                name=self.name,
                quote=quote_char or '',
            )
        else:
            field_sql = "{quote}{name}{quote}".format(
                name=self.name,
                quote=quote_char or '',
            )

        field_alias = getattr(self, 'alias', None)
        if not with_alias or field_alias is None:
            return field_sql

        return alias_sql(field_sql, field_alias, quote_char)


class Star(Field):
    def __init__(self, table=None):
        super(Star, self).__init__('*', table=table)

    def get_sql(self, with_alias=False, with_namespace=False, quote_char=None, **kwargs):
        if self.table and (with_namespace or self.table.alias):
            return "{quote}{namespace}{quote}.*".format(
                namespace=self.table.alias or self.table._table_name,
                quote=quote_char or ''
            )

        return '*'


class Tuple(Term):
    def __init__(self, *values):
        super(Tuple, self).__init__()
        self.values = [self.wrap_constant(value) for value in values]

    def __str__(self):
        return self.get_sql()

    def fields(self):
        return sum([value.fields() for value in self.values], [])

    def get_sql(self, **kwargs):
        return '({})'.format(
              ','.join(term.get_sql(**kwargs)
                       for term in self.values)
        )


class Array(Tuple):
    def get_sql(self, **kwargs):
        return '[{}]'.format(
              ','.join(term.get_sql(**kwargs)
                       for term in self.values)
        )


class BasicCriterion(Criterion):
    def __init__(self, comparator, left, right, alias=None):
        """
        A wrapper for a basic criterion such as equality or inequality. This wraps three parts, a left and right term
        and a comparator which defines the type of comparison.


        :param comparator:
            Type: Comparator
            This defines the type of comparison, such as {quote}={quote} or {quote}>{quote}.
        :param left:
            The term on the left side of the expression.
        :param right:
            The term on the right side of the expression.
        """
        super(BasicCriterion, self).__init__(alias)
        self.comparator = comparator
        self.left = left
        self.right = right

    @property
    def is_aggregate(self):
        return resolve_is_aggregate([term.is_aggregate for term in [self.left, self.right]])

    @property
    def tables_(self):
        return self.left.tables_ | self.right.tables_

    @builder
    def for_(self, table):
        self.left = self.left.for_(table)
        self.right = self.right.for_(table)

    def fields(self):
        return self.left.fields() + self.right.fields()

    def get_sql(self, with_alias=False, **kwargs):
        sql = '{left}{comparator}{right}'.format(
            comparator=self.comparator.value,
            left=self.left.get_sql(**kwargs),
            right=self.right.get_sql(**kwargs),
        )
        if with_alias and self.alias:
            return '{sql} "{alias}"'.format(sql=sql, alias=self.alias)

        return sql


class ContainsCriterion(Criterion):
    def __init__(self, term, container, alias=None):
        """
        A wrapper for a "IN" criterion.  This wraps two parts, a term and a container.  The term is the part of the
        expression that is checked for membership in the container.  The container can either be a list or a subquery.


        :param term:
            The term to assert membership for within the container.
        :param container:
            A list or subquery.
        """
        super(ContainsCriterion, self).__init__(alias)
        self.term = term
        self.container = container
        self._is_negated = False

    @property
    def tables_(self):
        return self.term.tables_

    def fields(self):
        return self.term.fields() if self.term.fields else []

    def get_sql(self, **kwargs):
        # FIXME escape
        return "{term} {not_}IN {container}".format(
            term=self.term.get_sql(**kwargs),
            container=self.container.get_sql(**kwargs),
            not_='NOT ' if self._is_negated else ''
        )

    def negate(self):
        self._is_negated = True
        return self


class BetweenCriterion(Criterion):
    def __init__(self, term, start, end, alias=None):
        super(BetweenCriterion, self).__init__(alias)
        self.term = term
        self.start = start
        self.end = end

    @property
    def tables_(self):
        return self.term.tables_

    @builder
    def for_(self, table):
        self.term = self.term.for_(table)

    def get_sql(self, **kwargs):
        # FIXME escape
        return "{term} BETWEEN {start} AND {end}".format(
            term=self.term.get_sql(**kwargs),
            start=self.start.get_sql(**kwargs),
            end=self.end.get_sql(**kwargs),
        )

    def fields(self):
        return self.term.fields() if self.term.fields else []


class NullCriterion(Criterion):
    def __init__(self, term, alias=None):
        super(NullCriterion, self).__init__(alias)
        self.term = term

    @property
    def tables_(self):
        return self.term.tables_

    @builder
    def for_(self, table):
        self.term = self.term.for_(table)

    def get_sql(self, **kwargs):
        return "{term} IS NULL".format(
            term=self.term.get_sql(**kwargs),
        )

    def fields(self):
        return self.term.fields() if self.term.fields else []


class ComplexCriterion(BasicCriterion):
    def fields(self):
        return self.left.fields() + self.right.fields()

    def get_sql(self, subcriterion=False, **kwargs):
        sql = '{left} {comparator} {right}'.format(
            comparator=self.comparator.value,
            left=self.left.get_sql(subcriterion=self.needs_brackets(self.left), **kwargs),
            right=self.right.get_sql(subcriterion=self.needs_brackets(self.right), **kwargs),
        )

        if subcriterion:
            return '({criterion})'.format(
                criterion=sql
            )

        return sql

    def needs_brackets(self, term):
        return isinstance(term, ComplexCriterion) and not term.comparator == self.comparator


class ArithmeticExpression(Term):
    """
    Wrapper for an arithmetic function.  Can be simple with two terms or complex with nested terms. Order of operations
    are also preserved.
    """

    mul_order = [Arithmetic.mul, Arithmetic.div]
    add_order = [Arithmetic.add, Arithmetic.sub]

    def __init__(self, operator, left, right, alias=None):
        """
        Wrapper for an arithmetic expression.

        :param operator:
            Type: Arithmetic
            An operator for the expression such as {quote}+{quote} or {quote}/{quote}

        :param left:
            The term on the left side of the expression.
        :param right:
            The term on the right side of the expression.
        :param alias:
            (Optional) an alias for the term which can be used inside a select statement.
        :return:
        """
        super(ArithmeticExpression, self).__init__(alias)
        self.operator = operator
        self.left = left
        self.right = right

    @property
    def is_aggregate(self):
        # True if both left and right terms are True or None. None if both terms are None. Otherwise, False
        return resolve_is_aggregate([self.left.is_aggregate, self.right.is_aggregate])

    @property
    def tables_(self):
        return self.left.tables_ | self.right.tables_

    @builder
    def for_(self, table):
        """
        Replaces the tables of this term for the table parameter provided.  Useful when reusing terms across queries.

        :param table:
            The table to replace with.
        :return:
            A copy of the term with it's table value replaced.
        """
        self.left = self.left.for_(table)
        self.right = self.right.for_(table)

    def fields(self):
        return self.left.fields() + self.right.fields()

    def get_sql(self, with_alias=False, **kwargs):
        is_mul = self.operator in self.mul_order
        is_left_add, is_right_add = [getattr(side, 'operator', None) in self.add_order
                                     for side in [self.left, self.right]]

        quote_char = kwargs.get('quote_char', None)
        arithmatic_sql = '{left}{operator}{right}'.format(
            operator=self.operator.value,
            left=("({})" if is_mul and is_left_add else "{}").format(self.left.get_sql(**kwargs)),
            right=("({})" if is_mul and is_right_add else "{}").format(self.right.get_sql(**kwargs)),
        )

        if not with_alias or self.alias is None:
            return arithmatic_sql

        return alias_sql(arithmatic_sql, self.alias, quote_char)


class Case(Term):
    def __init__(self, alias=None):
        super(Case, self).__init__(alias=alias)
        self._cases = []
        self._else = None

    @property
    def is_aggregate(self):
        # True if all cases are True or None. None all cases are None. Otherwise, False
        return resolve_is_aggregate([term.is_aggregate for _, term in self._cases]
                                    + [self._else.is_aggregate if self._else else None])

    @builder
    def when(self, criterion, term):
        self._cases.append((criterion, self.wrap_constant(term)))

    @builder
    def else_(self, term):
        self._else = self.wrap_constant(term)
        return self

    def get_sql(self, with_alias=False, **kwargs):
        if not self._cases:
            raise CaseException("At least one 'when' case is required for a CASE statement.")

        cases = " ".join('WHEN {when} THEN {then}'.format(
            when=criterion.get_sql(**kwargs),
            then=term.get_sql(**kwargs)
        ) for criterion, term in self._cases)
        else_ = (' ELSE {}'.format(self._else.get_sql(**kwargs))
                 if self._else
                 else '')

        case_sql = 'CASE {cases}{else_} END'.format(cases=cases, else_=else_)

        if not with_alias or self.alias is None:
            return case_sql

        return alias_sql(case_sql, self.alias, kwargs.get('quote_char'))

    def fields(self):
        fields = []

        for criterion, term in self._cases:
            fields += criterion.fields() + term.fields()

        if self._else is not None:
            fields += self._else.fields()

        return fields

    @property
    def tables_(self):
        tables = set()
        if self._cases:
            tables |= {table
                       for case in self._cases
                       for part in case
                       for table in part.tables_
                       if hasattr(part, 'tables_')}

        if self._else and hasattr(self._else, 'tables_'):
            tables |= {table
                       for table in self._else.tables_}

        return tables


class Not(Criterion):
    def __init__(self, term, alias=None):
        super(Not, self).__init__(alias=alias)
        self.term = term

    def fields(self):
        return self.term.fields() if self.term.fields else []

    def get_sql(self, quote_char=None, **kwargs):
        kwargs['subcriterion'] = True
        sql = "NOT {term}".format(term=self.term.get_sql(quote_char=quote_char,
                                                         **kwargs))
        return alias_sql(sql, self.alias, quote_char=quote_char)

    def __str__(self):
        return self.get_sql(quote_char='"')

    @ignore_copy
    def __getattr__(self, name):
        """
        Delegate method calls to the class wrapped by Not().
        Re-wrap methods on child classes of Term (e.g. isin, eg...) to retain 'NOT <term>' output.
        """
        item_func = getattr(self.term, name)

        if not inspect.ismethod(item_func):
            return item_func

        def inner(inner_self, *args, **kwargs):
            result = item_func(inner_self, *args, **kwargs)
            if isinstance(result, (Term,)):
                return Not(result)
            return result

        return inner

    @property
    def tables_(self):
        return self.term.tables_


class Function(Criterion):
    def __init__(self, name, *args, **kwargs):
        super(Function, self).__init__(kwargs.get('alias'))
        self.name = name
        self.args = [self.wrap_constant(param)
                     for param in args]
        self.schema = kwargs.get('schema')

    @property
    def tables_(self):
        return {table
                for param in self.args
                for table in param.tables_}

    def fields(self):
        return self.args

    @property
    def is_aggregate(self):
        """
        This is a shortcut thst assumes if a function has a single argument and that argument is aggregated, then this
        function is also aggregated. A more sophisticated approach is needed, however it is unclear how that might work.
        :returns:
            True if the function accepts one argument and that argument is aggregate.
        """
        return len(self.args) == 1 and self.args[0].is_aggregate

    @builder
    def for_(self, table):
        """
        Replaces the tables of this term for the table parameter provided.  Useful when reusing fields across queries.

        :param table:
            The table to replace with.
        :return:
            A copy of the field with it's table value replaced.
        """
        self.args = [param.for_(table) for param in self.args]

    def get_special_params_sql(self, **kwargs):
        pass

    def get_function_sql(self, **kwargs):
        special_params_sql = self.get_special_params_sql(**kwargs)

        return '{name}({args}{special})'.format(
            name=self.name,
            args=','.join(p.get_sql(with_alias=False, **kwargs)
                          if hasattr(p, 'get_sql')
                          else str(p)
                          for p in self.args),
            special=(' ' + special_params_sql) if special_params_sql else '',
        )

    def get_sql(self, with_alias=False, with_namespace=False, quote_char=None, **kwargs):
        # FIXME escape
        function_sql = self.get_function_sql(with_namespace=with_namespace, quote_char=quote_char)

        if self.schema is not None:
            function_sql = '{schema}.{function}' \
                .format(schema=self.schema.get_sql(quote_char=quote_char, **kwargs),
                        function=function_sql)

        if not with_alias or self.alias is None:
            return function_sql

        return alias_sql(function_sql, self.alias, quote_char)

    def fields(self):
        return [field
                for param in self.args
                if hasattr(param, 'fields')
                for field in param.fields()]


class AggregateFunction(Function):
    is_aggregate = True


class AnalyticFunction(Function):
    is_analytic = True

    def __init__(self, name, *args, **kwargs):
        super(AnalyticFunction, self).__init__(name, *args, **kwargs)
        self._partition = []
        self._orderbys = []

    @builder
    def over(self, *terms):
        self._partition += terms

    @builder
    def orderby(self, *terms, **kwargs):
        self._orderbys += [(term, kwargs.get('order'))
                           for term in terms]

    def _orderby_field(self, field, orient, **kwargs):
        if orient is None:
            return field.get_sql(**kwargs)

        return '{field} {orient}'.format(
            field=field.get_sql(**kwargs),
            orient=orient.value,
        )

    def get_partition_sql(self, **kwargs):
        terms = []
        if self._partition:
            terms.append('PARTITION BY {args}'.format(
                args=','.join(p.get_sql(**kwargs)
                              if hasattr(p, 'get_sql')
                              else str(p)
                              for p in self._partition)))

        if self._orderbys:
            terms.append('ORDER BY {orderby}'.format(
                orderby=','.join(
                    self._orderby_field(field, orient, **kwargs)
                    for field, orient in self._orderbys
                )))

        return ' '.join(terms)

    def get_function_sql(self, **kwargs):
        function_sql = super(AnalyticFunction, self).get_function_sql(**kwargs)
        partition_sql = self.get_partition_sql(**kwargs)

        if not partition_sql:
            return function_sql

        return '{function_sql} OVER({partition_sql})'.format(
            function_sql=function_sql,
            partition_sql=partition_sql
        )


class WindowFrameAnalyticFunction(AnalyticFunction):
    class Edge:
        def __init__(self, value=None):
            self.value = value

        def __str__(self):
            return '{value} {modifier}'.format(
                value=self.value or 'UNBOUNDED',
                modifier=self.modifier,
            )

    def __init__(self, name, *args, **kwargs):
        super(WindowFrameAnalyticFunction, self).__init__(name, *args, **kwargs)
        self.frame = None
        self.bound = None

    def _set_frame_and_bounds(self, frame, bound, and_bound):
        if self.frame or self.bound:
            raise AttributeError()

        self.frame = frame
        self.bound = (bound, and_bound) if and_bound else bound

    @builder
    def rows(self, bound, and_bound=None):
        self._set_frame_and_bounds('ROWS', bound, and_bound)

    @builder
    def range(self, bound, and_bound=None):
        self._set_frame_and_bounds('RANGE', bound, and_bound)

    def get_frame_sql(self):
        if not isinstance(self.bound, tuple):
            return '{frame} {bound}'.format(
                frame=self.frame,
                bound=self.bound
            )

        lower, upper = self.bound
        return '{frame} BETWEEN {lower} AND {upper}'.format(
            frame=self.frame,
            lower=lower,
            upper=upper,
        )

    def get_partition_sql(self, **kwargs):
        partition_sql = super(WindowFrameAnalyticFunction, self).get_partition_sql(**kwargs)

        if not self.frame and not self.bound:
            return partition_sql

        return '{over} {frame}'.format(
            over=partition_sql,
            frame=self.get_frame_sql()
        )


class IgnoreNullsAnalyticFunction(AnalyticFunction):
    def __init__(self, name, *args, **kwargs):
        super(IgnoreNullsAnalyticFunction, self).__init__(name, *args, **kwargs)
        self._ignore_nulls = False

    @builder
    def ignore_nulls(self):
        self._ignore_nulls = True

    def get_special_params_sql(self, **kwargs):
        if self._ignore_nulls:
            return 'IGNORE NULLS'

        # No special params unless ignoring nulls
        return None


class Interval(object):
    templates = {
        # MySQL requires no single quotes around the expr and unit
        Dialects.MYSQL: 'INTERVAL {expr} {unit}',

        # PostgreSQL, Redshift and Vertica require quotes around the expr and unit e.g. INTERVAL '1 week'
        Dialects.POSTGRESQL: 'INTERVAL \'{expr} {unit}\'',
        Dialects.REDSHIFT: 'INTERVAL \'{expr} {unit}\'',
        Dialects.VERTICA: 'INTERVAL \'{expr} {unit}\'',

        # Oracle requires just single quotes around the expr
        Dialects.ORACLE: 'INTERVAL \'{expr}\' {unit}'
    }

    units = ['years', 'months', 'days', 'hours', 'minutes', 'seconds', 'microseconds']
    labels = ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'MICROSECOND']

    trim_pattern = re.compile(r'(^0+\.)|(\.0+$)|(^[0\-.: ]+[\-: ])|([\-:. ][0\-.: ]+$)')

    def __init__(self, years=0, months=0, days=0, hours=0, minutes=0, seconds=0, microseconds=0, quarters=0, weeks=0,
                 dialect=None):
        self.dialect = dialect
        self.largest = None
        self.smallest = None

        if quarters:
            self.quarters = quarters
            return

        if weeks:
            self.weeks = weeks
            return

        for unit, label, value in zip(self.units, self.labels, [years, months, days,
                                                                hours, minutes, seconds, microseconds]):
            if value:
                setattr(self, unit, int(value))
                self.largest = self.largest or label
                self.smallest = label

    def __str__(self):
        return self.get_sql()

    def fields(self):
        return []

    def get_sql(self, **kwargs):
        dialect = self.dialect or kwargs.get('dialect')

        if self.largest == 'MICROSECOND':
            expr = getattr(self, 'microseconds')
            unit = 'MICROSECOND'

        elif hasattr(self, 'quarters'):
            expr = getattr(self, 'quarters')
            unit = 'QUARTER'

        elif hasattr(self, 'weeks'):
            expr = getattr(self, 'weeks')
            unit = 'WEEK'

        else:
            # Create the whole expression but trim out the unnecessary fields
            expr = "{years}-{months}-{days} {hours}:{minutes}:{seconds}.{microseconds}".format(
                    years=getattr(self, 'years', 0),
                    months=getattr(self, 'months', 0),
                    days=getattr(self, 'days', 0),
                    hours=getattr(self, 'hours', 0),
                    minutes=getattr(self, 'minutes', 0),
                    seconds=getattr(self, 'seconds', 0),
                    microseconds=getattr(self, 'microseconds', 0),
                )
            expr = self.trim_pattern.sub('', expr)

            unit = '{largest}_{smallest}'.format(
                largest=self.largest,
                smallest=self.smallest,
            ) if self.largest != self.smallest else self.largest

        return self.templates.get(dialect, 'INTERVAL \'{expr} {unit}\'') \
            .format(expr=expr, unit=unit)


class Pow(Function):
    def __init__(self, term, exponent, alias=None):
        super(Pow, self).__init__('POW', term, exponent, alias=alias)


class Mod(Function):
    def __init__(self, term, modulus, alias=None):
        super(Mod, self).__init__('MOD', term, modulus, alias=alias)


class Rollup(Function):
    def __init__(self, *terms):
        super(Rollup, self).__init__('ROLLUP', *terms)


class Psuedocolumn(Term):
    """
    Represents a pseudocolumn
    """

    def __init__(self, name):
        self.name = name

    def to_sql(self, **kwargs):
        return self.name
