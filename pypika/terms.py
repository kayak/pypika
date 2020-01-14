import inspect
import re
from datetime import date
from enum import Enum
from typing import (
    Iterable,
    Union,
)

from pypika.enums import (
    Arithmetic,
    Boolean,
    Dialects,
    Equality,
    JSONOperators,
    Matching,
)
from pypika.utils import (
    CaseException,
    FunctionException,
    builder,
    format_alias_sql,
    format_quotes,
    ignore_copy,
    resolve_is_aggregate,
)

try:
    basestring
except NameError:
    basestring = str

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class Node:
    def nodes_(self):
        yield self

    def find_(self, type):
        return [node for node in self.nodes_() if isinstance(node, type)]


class Term(Node):
    is_aggregate = False

    def __init__(self, alias=None):
        self.alias = alias

    @builder
    def as_(self, alias):
        self.alias = alias

    @property
    def tables_(self):
        from pypika import Table

        return set(self.find_(Table))

    def fields_(self):
        return set(self.find_(Field))

    @staticmethod
    def wrap_constant(val, wrapper_cls=None):
        """
        Used for wrapping raw inputs such as numbers in Criterions and Operator.

        For example, the expression F('abc')+1 stores the integer part in a ValueWrapper object.

        :param val:
            Any value.
        :param wrapper_cls:
            A pypika class which wraps a constant value so it can be handled as a component of the query.
        :return:
            Raw string, number, or decimal values will be returned in a ValueWrapper.  Fields and other parts of the
            querybuilder will be returned as inputted.

        """
        from .queries import QueryBuilder

        if isinstance(val, Node):
            return val
        if val is None:
            return NullValue()
        if isinstance(val, list):
            return Array(*val)
        if isinstance(val, tuple):
            return Tuple(*val)

        # Need to default here to avoid the recursion. ValueWrapper extends this class.
        wrapper_cls = wrapper_cls or ValueWrapper
        return wrapper_cls(val)

    @staticmethod
    def wrap_json(val, wrapper_cls=None):
        from .queries import QueryBuilder

        if isinstance(val, (Term, QueryBuilder, Interval)):
            return val
        if val is None:
            return NullValue()
        if isinstance(val, (str, int, bool)):
            wrapper_cls = wrapper_cls or ValueWrapper
            return wrapper_cls(val)

        return JSON(val)

    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.
        The base implementation returns self because not all terms have a table property.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            Self.
        """
        return self

    def eq(self, other):
        return self == other

    def isnull(self):
        return NullCriterion(self)

    def notnull(self):
        return self.isnull().negate()

    def bitwiseand(self, value):
        return BitwiseAndCriterion(self, value)

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
        return BetweenCriterion(
            self, self.wrap_constant(lower), self.wrap_constant(upper)
        )

    def isin(self, arg):
        if isinstance(arg, (list, tuple, set)):
            return ContainsCriterion(
                self, Tuple(*[self.wrap_constant(value) for value in arg])
            )
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
        return self.get_sql(quote_char='"', secondary_quote_char="'")

    def __hash__(self):
        return hash(self.get_sql(with_alias=True))

    def get_sql(self, **kwargs):
        raise NotImplementedError()


class Parameter(Term):
    is_aggregate = None

    def __init__(self, placeholder):
        super(Parameter, self).__init__()
        self.placeholder = placeholder

    def get_sql(self, **kwargs):
        return str(self.placeholder)


class Negative(Term):
    def __init__(self, term):
        super(Negative, self).__init__()
        self.term = term

    @property
    def is_aggregate(self):
        return self.term.is_aggregate

    def get_sql(self, **kwargs):
        return "-{term}".format(term=self.term.get_sql(**kwargs))


class ValueWrapper(Term):
    is_aggregate = None

    def __init__(self, value, alias=None):
        super(ValueWrapper, self).__init__(alias)
        self.value = value

    def get_value_sql(self, **kwargs):
        quote_char = kwargs.get("secondary_quote_char") or ""

        # FIXME escape values
        if isinstance(self.value, Term):
            return self.value.get_sql(**kwargs)
        if isinstance(self.value, Enum):
            return self.value.value
        if isinstance(self.value, date):
            value = self.value.isoformat()
            return format_quotes(value, quote_char)
        if isinstance(self.value, basestring):
            value = self.value.replace(quote_char, quote_char * 2)
            return format_quotes(value, quote_char)
        if isinstance(self.value, bool):
            return str.lower(str(self.value))
        if self.value is None:
            return "null"
        return str(self.value)

    def get_sql(self, quote_char=None, secondary_quote_char="'", **kwargs):
        sql = self.get_value_sql(
            quote_char=quote_char, secondary_quote_char=secondary_quote_char, **kwargs
        )
        return format_alias_sql(sql, self.alias, quote_char=quote_char, **kwargs)


class JSON(Term):
    table = None

    def __init__(self, value, alias=None):
        super().__init__(alias)
        self.value = value

    def _recursive_get_sql(self, value, **kwargs):
        if isinstance(value, dict):
            return self._get_dict_sql(value, **kwargs)
        if isinstance(value, list):
            return self._get_list_sql(value, **kwargs)
        if isinstance(value, str):
            return self._get_str_sql(value, **kwargs)
        return str(value)

    def _get_dict_sql(self, value, **kwargs):
        pairs = [
            "{key}:{value}".format(
                key=self._recursive_get_sql(k, **kwargs),
                value=self._recursive_get_sql(v, **kwargs),
            )
            for k, v in value.items()
        ]
        return "".join(["{", ",".join(pairs), "}"])

    def _get_list_sql(self, value, **kwargs):
        pairs = [self._recursive_get_sql(v, **kwargs) for v in value]
        return "".join(["[", ",".join(pairs), "]"])

    @staticmethod
    def _get_str_sql(value, quote_char='"', **kwargs):
        return format_quotes(value, quote_char)

    def get_sql(self, secondary_quote_char="'", **kwargs):
        return format_quotes(self._recursive_get_sql(self.value), secondary_quote_char)

    def get_json_value(self, key_or_index: Union[str, int]):
        return BasicCriterion(
            JSONOperators.GET_JSON_VALUE, self, self.wrap_constant(key_or_index)
        )

    def get_text_value(self, key_or_index: Union[str, int]):
        return BasicCriterion(
            JSONOperators.GET_TEXT_VALUE, self, self.wrap_constant(key_or_index)
        )

    def get_path_json_value(self, path_json: str):
        return BasicCriterion(
            JSONOperators.GET_PATH_JSON_VALUE, self, self.wrap_json(path_json)
        )

    def get_path_text_value(self, path_json: str):
        return BasicCriterion(
            JSONOperators.GET_PATH_TEXT_VALUE, self, self.wrap_json(path_json)
        )

    def has_key(self, other):
        return BasicCriterion(JSONOperators.HAS_KEY, self, self.wrap_json(other))

    def contains(self, other):
        return BasicCriterion(JSONOperators.CONTAINS, self, self.wrap_json(other))

    def contained_by(self, other):
        return BasicCriterion(JSONOperators.CONTAINED_BY, self, self.wrap_json(other))

    def has_keys(self, other: Iterable):
        return BasicCriterion(JSONOperators.HAS_KEYS, self, Array(*other))

    def has_any_keys(self, other: Iterable):
        return BasicCriterion(JSONOperators.HAS_ANY_KEYS, self, Array(*other))


class Values(Term):
    def __init__(
        self, field,
    ):
        super().__init__(None)
        self.field = Field(field) if not isinstance(field, Field) else field

    def get_sql(self, quote_char=None, **kwargs):
        return "VALUES({value})".format(
            value=self.field.get_sql(quote_char=quote_char, **kwargs)
        )


class NullValue(Term):
    def get_sql(self, **kwargs):
        sql = "NULL"
        return format_alias_sql(sql, self.alias, **kwargs)


class Criterion(Term):
    def __and__(self, other):
        return ComplexCriterion(Boolean.and_, self, other)

    def __or__(self, other):
        return ComplexCriterion(Boolean.or_, self, other)

    def __xor__(self, other):
        return ComplexCriterion(Boolean.xor_, self, other)

    @staticmethod
    def any(terms=()):
        crit = EmptyCriterion()

        for term in terms:
            crit |= term

        return crit

    @staticmethod
    def all(terms=()):
        crit = EmptyCriterion()

        for term in terms:
            crit &= term

        return crit

    def get_sql(self):
        raise NotImplementedError()


class EmptyCriterion:
    is_aggregate = None
    tables_ = set()

    def __and__(self, other):
        return other

    def __or__(self, other):
        return other

    def __xor__(self, other):
        return other


class Field(Criterion, JSON):
    def __init__(self, name, alias=None, table=None):
        super(Field, self).__init__(alias)
        self.name = name
        self.table = table

    def nodes_(self):
        yield self
        if self.table is not None:
            yield from self.table.nodes_()

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the field with the tables replaced.
        """
        self.table = new_table if self.table == current_table else self.table

    def get_sql(
        self,
        with_alias=False,
        with_namespace=False,
        quote_char=None,
        secondary_quote_char="'",
        **kwargs
    ):
        field_sql = format_quotes(self.name, quote_char)

        # Need to add namespace if the table has an alias
        if self.table and (with_namespace or self.table.alias):
            table_name = self.table.get_table_name()
            field_sql = "{namespace}.{name}".format(
                namespace=format_quotes(table_name, quote_char), name=field_sql,
            )

        field_alias = getattr(self, "alias", None)
        if with_alias:
            return format_alias_sql(
                field_sql, field_alias, quote_char=quote_char, **kwargs
            )

        return field_sql


class Index(Term):
    def __init__(self, name, alias=None):
        super(Index, self).__init__(alias)
        self.name = name

    def get_sql(self, quote_char=None, **kwargs):
        return format_quotes(self.name, quote_char)


class Star(Field):
    def __init__(self, table=None):
        super(Star, self).__init__("*", table=table)

    def nodes_(self):
        yield self
        if self.table is not None:
            yield from self.table.nodes_()

    def get_sql(
        self, with_alias=False, with_namespace=False, quote_char=None, **kwargs
    ):
        if self.table and (with_namespace or self.table.alias):
            namespace = self.table.alias or getattr(self.table, "_table_name")
            return "{}.*".format(format_quotes(namespace, quote_char))

        return "*"


class Tuple(Criterion):
    def __init__(self, *values):
        super(Tuple, self).__init__()
        self.values = [self.wrap_constant(value) for value in values]

    def nodes_(self):
        yield self
        for value in self.values:
            yield from value.nodes_()

    def get_sql(self, **kwargs):
        return "({})".format(",".join(term.get_sql(**kwargs) for term in self.values))

    @property
    def is_aggregate(self):
        return all([value.is_aggregate for value in self.values])

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the field with the tables replaced.
        """
        self.values = [
            [value.replace_table(current_table, new_table) for value in value_list]
            for value_list in self.values
        ]


class Array(Tuple):
    def get_sql(self, **kwargs):
        dialect = kwargs.get("dialect", None)
        template = (
            "ARRAY[{}]"
            if dialect in (Dialects.POSTGRESQL, Dialects.REDSHIFT)
            else "[{}]"
        )

        return template.format(",".join(term.get_sql(**kwargs) for term in self.values))


class Bracket(Tuple):
    def __init__(self, term):
        super(Bracket, self).__init__(term)

    def get_sql(self, **kwargs):
        sql = super(Bracket, self).get_sql(**kwargs)
        return format_alias_sql(sql=sql, alias=self.alias, **kwargs)


class NestedCriterion(Criterion):
    def __init__(self, comparator, nested_comparator, left, right, nested, alias=None):
        super().__init__(alias)
        self.left = left
        self.comparator = comparator
        self.nested_comparator = nested_comparator
        self.right = right
        self.nested = nested

    def nodes_(self):
        yield self
        yield from self.right.nodes_()
        yield from self.left.nodes_()
        yield from self.nested.nodes_()

    @property
    def is_aggregate(self):
        return resolve_is_aggregate(
            [term.is_aggregate for term in [self.left, self.right, self.nested]]
        )

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.left = self.left.replace_table(current_table, new_table)
        self.right = self.right.replace_table(current_table, new_table)
        self.nested = self.right.replace_table(current_table, new_table)

    def get_sql(self, with_alias=False, **kwargs):
        sql = "{left}{comparator}{right}{nested_comparator}{nested}".format(
            left=self.left.get_sql(**kwargs),
            comparator=self.comparator.value,
            right=self.right.get_sql(**kwargs),
            nested_comparator=self.nested_comparator.value,
            nested=self.nested.get_sql(**kwargs),
        )

        if with_alias:
            return format_alias_sql(sql=sql, alias=self.alias, **kwargs)

        return sql


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

    def nodes_(self):
        yield self
        yield from self.right.nodes_()
        yield from self.left.nodes_()

    @property
    def is_aggregate(self):
        return resolve_is_aggregate(
            [term.is_aggregate for term in [self.left, self.right]]
        )

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.left = self.left.replace_table(current_table, new_table)
        self.right = self.right.replace_table(current_table, new_table)

    def get_sql(self, quote_char='"', with_alias=False, **kwargs):
        sql = "{left}{comparator}{right}".format(
            comparator=self.comparator.value,
            left=self.left.get_sql(quote_char=quote_char, **kwargs),
            right=self.right.get_sql(quote_char=quote_char, **kwargs),
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

    def nodes_(self):
        yield self
        yield from self.term.nodes_()
        yield from self.container.nodes_()

    @property
    def is_aggregate(self):
        return self.term.is_aggregate

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)

    def get_sql(self, subquery=None, **kwargs):
        return "{term} {not_}IN {container}".format(
            term=self.term.get_sql(**kwargs),
            container=self.container.get_sql(subquery=True, **kwargs),
            not_="NOT " if self._is_negated else "",
        )

    @builder
    def negate(self):
        self._is_negated = True


class BetweenCriterion(Criterion):
    def __init__(self, term, start, end, alias=None):
        super(BetweenCriterion, self).__init__(alias)
        self.term = term
        self.start = start
        self.end = end

    def nodes_(self):
        yield self
        yield from self.term.nodes_()
        yield from self.start.nodes_()
        yield from self.end.nodes_()

    @property
    def is_aggregate(self):
        return self.term.is_aggregate

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)

    def get_sql(self, **kwargs):
        # FIXME escape
        return "{term} BETWEEN {start} AND {end}".format(
            term=self.term.get_sql(**kwargs),
            start=self.start.get_sql(**kwargs),
            end=self.end.get_sql(**kwargs),
        )


class BitwiseAndCriterion(Criterion):
    def __init__(self, term, value, alias=None):
        super(BitwiseAndCriterion, self).__init__(alias)
        self.term = term
        self.value = value

    def nodes_(self):
        yield self
        yield from self.term.nodes_()
        yield from self.value.nodes_()

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)

    def get_sql(self, **kwargs):
        return "({term} & {value})".format(
            term=self.term.get_sql(**kwargs), value=self.value,
        )


class NullCriterion(Criterion):
    def __init__(self, term, alias=None):
        super(NullCriterion, self).__init__(alias)
        self.term = term

    def nodes_(self):
        yield self
        yield from self.term.nodes_()

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)

    def get_sql(self, **kwargs):
        return "{term} IS NULL".format(term=self.term.get_sql(**kwargs),)


class ComplexCriterion(BasicCriterion):
    def get_sql(self, subcriterion=False, **kwargs):
        sql = "{left} {comparator} {right}".format(
            comparator=self.comparator.value,
            left=self.left.get_sql(
                subcriterion=self.needs_brackets(self.left), **kwargs
            ),
            right=self.right.get_sql(
                subcriterion=self.needs_brackets(self.right), **kwargs
            ),
        )

        if subcriterion:
            return "({criterion})".format(criterion=sql)

        return sql

    def needs_brackets(self, term):
        return (
            isinstance(term, ComplexCriterion)
            and not term.comparator == self.comparator
        )


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

    def nodes_(self):
        yield self
        yield from self.left.nodes_()
        yield from self.right.nodes_()

    @property
    def is_aggregate(self):
        # True if both left and right terms are True or None. None if both terms are None. Otherwise, False
        return resolve_is_aggregate([self.left.is_aggregate, self.right.is_aggregate])

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the term with the tables replaced.
        """
        self.left = self.left.replace_table(current_table, new_table)
        self.right = self.right.replace_table(current_table, new_table)

    def get_sql(self, with_alias=False, **kwargs):
        is_mul = self.operator in self.mul_order
        is_left_add, is_right_add = [
            getattr(side, "operator", None) in self.add_order
            for side in [self.left, self.right]
        ]

        arithmetic_sql = "{left}{operator}{right}".format(
            operator=self.operator.value,
            left=("({})" if is_mul and is_left_add else "{}").format(
                self.left.get_sql(**kwargs)
            ),
            right=("({})" if is_mul and is_right_add else "{}").format(
                self.right.get_sql(**kwargs)
            ),
        )

        if with_alias:
            return format_alias_sql(arithmetic_sql, self.alias, **kwargs)

        return arithmetic_sql


class Case(Term):
    def __init__(self, alias=None):
        super(Case, self).__init__(alias=alias)
        self._cases = []
        self._else = None

    def nodes_(self):
        yield self

        for criterion, term in self._cases:
            yield from criterion.nodes_()
            yield from term.nodes_()

        if self._else is not None:
            yield from self._else.nodes_()

    @property
    def is_aggregate(self):
        # True if all cases are True or None. None all cases are None. Otherwise, False
        return resolve_is_aggregate(
            [term.is_aggregate for _, term in self._cases]
            + [self._else.is_aggregate if self._else else None]
        )

    @builder
    def when(self, criterion, term):
        self._cases.append((criterion, self.wrap_constant(term)))

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the term with the tables replaced.
        """
        self._cases = [
            [
                criterion.replace_table(current_table, new_table),
                term.replace_table(current_table, new_table),
            ]
            for criterion, term in self._cases
        ]
        self._else = (
            self._else.replace_table(current_table, new_table) if self._else else None
        )

    @builder
    def else_(self, term):
        self._else = self.wrap_constant(term)
        return self

    def get_sql(self, with_alias=False, **kwargs):
        if not self._cases:
            raise CaseException(
                "At least one 'when' case is required for a CASE statement."
            )

        cases = " ".join(
            "WHEN {when} THEN {then}".format(
                when=criterion.get_sql(**kwargs), then=term.get_sql(**kwargs)
            )
            for criterion, term in self._cases
        )
        else_ = " ELSE {}".format(self._else.get_sql(**kwargs)) if self._else else ""

        case_sql = "CASE {cases}{else_} END".format(cases=cases, else_=else_)

        if with_alias:
            return format_alias_sql(case_sql, self.alias, **kwargs)

        return case_sql


class Not(Criterion):
    def __init__(self, term, alias=None):
        super(Not, self).__init__(alias=alias)
        self.term = term

    def nodes_(self):
        yield self
        yield from self.term.nodes_()

    def get_sql(self, **kwargs):
        kwargs["subcriterion"] = True
        sql = "NOT {term}".format(term=self.term.get_sql(**kwargs))
        return format_alias_sql(sql, self.alias, **kwargs)

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

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)


class CustomFunction:
    def __init__(self, name, params=None):
        self.name = name
        self.params = params

    def __call__(self, *args, **kwargs):
        if not self._has_params():
            return Function(self.name, alias=kwargs.get("alias"))

        if not self._is_valid_function_call(*args):
            raise FunctionException(
                "Function {name} require these arguments ({params}), ({args}) passed".format(
                    name=self.name,
                    params=", ".join(str(p) for p in self.params),
                    args=", ".join(str(p) for p in args),
                )
            )

        return Function(self.name, *args, alias=kwargs.get("alias"))

    def _has_params(self):
        return self.params is not None

    def _is_valid_function_call(self, *args):
        return len(args) == len(self.params)


class Function(Criterion):
    def __init__(self, name, *args, **kwargs):
        super(Function, self).__init__(kwargs.get("alias"))
        self.name = name
        self.args = [self.wrap_constant(param) for param in args]
        self.schema = kwargs.get("schema")

    def nodes_(self):
        yield self
        for arg in self.args:
            yield from arg.nodes_()

    @property
    def is_aggregate(self):
        """
        This is a shortcut that assumes if a function has a single argument and that argument is aggregated, then this
        function is also aggregated. A more sophisticated approach is needed, however it is unclear how that might work.
        :returns:
            True if the function accepts one argument and that argument is aggregate.
        """
        return len(self.args) == 1 and self.args[0].is_aggregate

    @builder
    def replace_table(self, current_table, new_table):
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.args = [
            param.replace_table(current_table, new_table) for param in self.args
        ]

    def get_special_params_sql(self, **kwargs):
        pass

    def get_function_sql(self, **kwargs):
        special_params_sql = self.get_special_params_sql(**kwargs)

        return "{name}({args}{special})".format(
            name=self.name,
            args=",".join(
                p.get_sql(with_alias=False, **kwargs)
                if hasattr(p, "get_sql")
                else str(p)
                for p in self.args
            ),
            special=(" " + special_params_sql) if special_params_sql else "",
        )

    def get_sql(
        self,
        with_alias=False,
        with_namespace=False,
        quote_char=None,
        dialect=None,
        **kwargs
    ):
        # FIXME escape
        function_sql = self.get_function_sql(
            with_namespace=with_namespace, quote_char=quote_char, dialect=dialect
        )

        if self.schema is not None:
            function_sql = "{schema}.{function}".format(
                schema=self.schema.get_sql(
                    quote_char=quote_char, dialect=dialect, **kwargs
                ),
                function=function_sql,
            )

        if with_alias:
            return format_alias_sql(
                function_sql, self.alias, quote_char=quote_char, **kwargs
            )

        return function_sql


class AggregateFunction(Function):
    is_aggregate = True


class AnalyticFunction(Function):
    is_analytic = True

    def __init__(self, name, *args, **kwargs):
        super(AnalyticFunction, self).__init__(name, *args, **kwargs)
        self._partition = []
        self._orderbys = []
        self._include_over = False

    @builder
    def over(self, *terms):
        self._include_over = True
        self._partition += terms

    @builder
    def orderby(self, *terms, **kwargs):
        self._include_over = True
        self._orderbys += [(term, kwargs.get("order")) for term in terms]

    def _orderby_field(self, field, orient, **kwargs):
        if orient is None:
            return field.get_sql(**kwargs)

        return "{field} {orient}".format(
            field=field.get_sql(**kwargs), orient=orient.value,
        )

    def get_partition_sql(self, **kwargs):
        terms = []
        if self._partition:
            terms.append(
                "PARTITION BY {args}".format(
                    args=",".join(
                        p.get_sql(**kwargs) if hasattr(p, "get_sql") else str(p)
                        for p in self._partition
                    )
                )
            )

        if self._orderbys:
            terms.append(
                "ORDER BY {orderby}".format(
                    orderby=",".join(
                        self._orderby_field(field, orient, **kwargs)
                        for field, orient in self._orderbys
                    )
                )
            )

        return " ".join(terms)

    def get_function_sql(self, **kwargs):
        function_sql = super(AnalyticFunction, self).get_function_sql(**kwargs)
        partition_sql = self.get_partition_sql(**kwargs)

        if not self._include_over:
            return function_sql

        return "{function_sql} OVER({partition_sql})".format(
            function_sql=function_sql, partition_sql=partition_sql
        )


class WindowFrameAnalyticFunction(AnalyticFunction):
    class Edge:
        def __init__(self, value=None):
            self.value = value

        def __str__(self):
            return "{value} {modifier}".format(
                value=self.value or "UNBOUNDED", modifier=self.modifier,
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
        self._set_frame_and_bounds("ROWS", bound, and_bound)

    @builder
    def range(self, bound, and_bound=None):
        self._set_frame_and_bounds("RANGE", bound, and_bound)

    def get_frame_sql(self):
        if not isinstance(self.bound, tuple):
            return "{frame} {bound}".format(frame=self.frame, bound=self.bound)

        lower, upper = self.bound
        return "{frame} BETWEEN {lower} AND {upper}".format(
            frame=self.frame, lower=lower, upper=upper,
        )

    def get_partition_sql(self, **kwargs):
        partition_sql = super(WindowFrameAnalyticFunction, self).get_partition_sql(
            **kwargs
        )

        if not self.frame and not self.bound:
            return partition_sql

        return "{over} {frame}".format(over=partition_sql, frame=self.get_frame_sql())


class IgnoreNullsAnalyticFunction(AnalyticFunction):
    def __init__(self, name, *args, **kwargs):
        super(IgnoreNullsAnalyticFunction, self).__init__(name, *args, **kwargs)
        self._ignore_nulls = False

    @builder
    def ignore_nulls(self):
        self._ignore_nulls = True

    def get_special_params_sql(self, **kwargs):
        if self._ignore_nulls:
            return "IGNORE NULLS"

        # No special params unless ignoring nulls
        return None


class Interval(Node):
    templates = {
        # MySQL requires no single quotes around the expr and unit
        Dialects.MYSQL: "INTERVAL {expr} {unit}",
        # PostgreSQL, Redshift and Vertica require quotes around the expr and unit e.g. INTERVAL '1 week'
        Dialects.POSTGRESQL: "INTERVAL '{expr} {unit}'",
        Dialects.REDSHIFT: "INTERVAL '{expr} {unit}'",
        Dialects.VERTICA: "INTERVAL '{expr} {unit}'",
        # Oracle requires just single quotes around the expr
        Dialects.ORACLE: "INTERVAL '{expr}' {unit}",
    }

    units = ["years", "months", "days", "hours", "minutes", "seconds", "microseconds"]
    labels = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "MICROSECOND"]

    trim_pattern = re.compile(r"(^0+\.)|(\.0+$)|(^[0\-.: ]+[\-: ])|([\-:. ][0\-.: ]+$)")

    def __init__(
        self,
        years=0,
        months=0,
        days=0,
        hours=0,
        minutes=0,
        seconds=0,
        microseconds=0,
        quarters=0,
        weeks=0,
        dialect=None,
    ):
        self.dialect = dialect
        self.largest = None
        self.smallest = None

        if quarters:
            self.quarters = quarters
            return

        if weeks:
            self.weeks = weeks
            return

        for unit, label, value in zip(
            self.units,
            self.labels,
            [years, months, days, hours, minutes, seconds, microseconds],
        ):
            if value:
                setattr(self, unit, int(value))
                self.largest = self.largest or label
                self.smallest = label

    def __str__(self):
        return self.get_sql()

    def get_sql(self, **kwargs):
        dialect = self.dialect or kwargs.get("dialect")

        if self.largest == "MICROSECOND":
            expr = getattr(self, "microseconds")
            unit = "MICROSECOND"

        elif hasattr(self, "quarters"):
            expr = getattr(self, "quarters")
            unit = "QUARTER"

        elif hasattr(self, "weeks"):
            expr = getattr(self, "weeks")
            unit = "WEEK"

        else:
            # Create the whole expression but trim out the unnecessary fields
            expr = "{years}-{months}-{days} {hours}:{minutes}:{seconds}.{microseconds}".format(
                years=getattr(self, "years", 0),
                months=getattr(self, "months", 0),
                days=getattr(self, "days", 0),
                hours=getattr(self, "hours", 0),
                minutes=getattr(self, "minutes", 0),
                seconds=getattr(self, "seconds", 0),
                microseconds=getattr(self, "microseconds", 0),
            )
            expr = self.trim_pattern.sub("", expr)

            unit = (
                "{largest}_{smallest}".format(
                    largest=self.largest, smallest=self.smallest,
                )
                if self.largest != self.smallest
                else self.largest
            )

        return self.templates.get(dialect, "INTERVAL '{expr} {unit}'").format(
            expr=expr, unit=unit
        )


class Pow(Function):
    def __init__(self, term, exponent, alias=None):
        super(Pow, self).__init__("POW", term, exponent, alias=alias)


class Mod(Function):
    def __init__(self, term, modulus, alias=None):
        super(Mod, self).__init__("MOD", term, modulus, alias=alias)


class Rollup(Function):
    def __init__(self, *terms):
        super(Rollup, self).__init__("ROLLUP", *terms)


class PseudoColumn(Term):
    """
    Represents a pseudo column (a "column" which yields a value when selected
    but is not actually a real table column).
    """

    def __init__(self, name):
        self.name = name

    def get_sql(self, **kwargs):
        return self.name
