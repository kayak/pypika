# coding: utf8
import re
from datetime import date

from pypika.enums import Boolean, Equality, Arithmetic, Matching
from pypika.utils import CaseException, immutable

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"
__version__ = "0.0.1"


class Term(object):
    @staticmethod
    def _wrap(val):
        """
        Used for wrapping raw inputs such as numbers in Criterions and Operator.

        For example, the expression F('abc')+1 stores the integer part in a ValueWrapper object.

        :param val:
            Any value.
        :return:
            Raw string, number, or decimal values will be returned in a ValueWrapper.  Fields and other parts of the
            querybuilder will be returned as inputted.

        """
        from .queries import Query
        if isinstance(val, Term) or isinstance(val, Query):
            return val

        return ValueWrapper(val)

    def for_(self, table):
        """
        Replaces the tables of this term for the table parameter provided.  Useful when reusing fields across queries.

        :param table:
            The table to replace with.
        :return:
            A copy of the field with it's table value replaced.
        """
        raise NotImplementedError()

    def fields(self):
        return [self]

    def eq(self, other):
        return self == other

    def isnull(self):
        return NullCriterion(self, True)

    def notnull(self):
        return NullCriterion(self, False)

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
        return BasicCriterion(Matching.like, self, self._wrap(expr))

    def regex(self, pattern):
        return BasicCriterion(Matching.regex, self, self._wrap(pattern))

    def bin_regex(self, pattern):
        return BasicCriterion(Matching.bin_regex, self, self._wrap(pattern))

    def __add__(self, other):
        return ArithmeticFunction(Arithmetic.add, self, self._wrap(other))

    def __sub__(self, other):
        return ArithmeticFunction(Arithmetic.sub, self, self._wrap(other))

    def __mul__(self, other):
        return ArithmeticFunction(Arithmetic.mul, self, self._wrap(other))

    def __div__(self, other):
        # Required for Python2
        return self.__truediv__(other)

    def __truediv__(self, other):
        return ArithmeticFunction(Arithmetic.div, self, self._wrap(other))

    def __pow__(self, other):
        return Functions.Pow(self, other)

    def __mod__(self, other):
        return Functions.Mod(self, other)

    def __radd__(self, other):
        return ArithmeticFunction(Arithmetic.add, self._wrap(other), self)

    def __rsub__(self, other):
        return ArithmeticFunction(Arithmetic.sub, self._wrap(other), self)

    def __rmul__(self, other):
        return ArithmeticFunction(Arithmetic.mul, self._wrap(other), self)

    def __rdiv__(self, other):
        # Required for Python2
        return self.__rtruediv__(other)

    def __rtruediv__(self, other):
        return ArithmeticFunction(Arithmetic.div, self._wrap(other), self)

    def __eq__(self, other):
        return BasicCriterion(Equality.eq, self, self._wrap(other))

    def __ne__(self, other):
        return BasicCriterion(Equality.ne, self, self._wrap(other))

    def __gt__(self, other):
        return BasicCriterion(Equality.gt, self, self._wrap(other))

    def __ge__(self, other):
        return BasicCriterion(Equality.gte, self, self._wrap(other))

    def __lt__(self, other):
        return BasicCriterion(Equality.lt, self, self._wrap(other))

    def __le__(self, other):
        return BasicCriterion(Equality.lte, self, self._wrap(other))


class ValueWrapper(Term):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        # FIXME escape values
        if isinstance(self.value, date):
            return "'%s'" % self.value.isoformat()
        elif isinstance(self.value, str):
            return "'%s'" % self.value
        elif isinstance(self.value, bool):
            return str.lower(str(self.value))
        return str(self.value)

    def fields(self):
        return []


class Field(Term):
    def __init__(self, name, alias=None, table=None):
        self.name = name
        self.table = table
        self._alias = alias

    def between(self, lower, upper):
        return BetweenCriterion(self, self._wrap(lower), self._wrap(upper))

    def isin(self, arg):
        if isinstance(arg, (list, tuple, set)):
            return ContainsCriterion(self, ListField([self._wrap(value) for value in arg]))
        return ContainsCriterion(self, arg)

    @immutable
    def as_(self, alias):
        self._alias = alias
        return self

    @immutable
    def for_(self, table):
        self.table = table
        return self

    def __str__(self):
        # FIXME escape
        namespace = ('%s.' % self.table._alias
                     if self.table is not None
                        and self.table._alias is not None
                     else '')
        return "{namespace}{name}".format(
            namespace=namespace,
            name=self.name,
        )

    def __getitem__(self, item):
        if not isinstance(item, slice):
            raise TypeError("Field' object is not subscriptable")
        return self.between(item.start, item.stop)


class Star(Field):
    def __init__(self, table=None):
        super(Star, self).__init__('*', alias=None, table=table)


class ListField(object):
    def __init__(self, values):
        self.values = values

    def __str__(self):
        return ','.join(map(str, self.values))


class Criterion(object):
    def __and__(self, other):
        return ComplexCriterion(Boolean.and_, self, other)

    def __or__(self, other):
        return ComplexCriterion(Boolean.or_, self, other)

    def __xor__(self, other):
        return ComplexCriterion(Boolean.xor_, self, other)

    def fields(self):
        raise NotImplementedError()


class BasicCriterion(Criterion):
    def __init__(self, comparator, left, right):
        """
        A wrapper for a basic criterion such as equality or inequality.  This wraps three parts, a left and right term
        and a comparator which defines the type of comparison.


        :param comparator:
            Type: Comparator
            This defines the type of comparison, such as `=` or `>`.
        :param left:
            The term on the left side of the expression.
        :param right:
            The term on the right side of the expression.
        """
        self.comparator = comparator
        self.left = left
        self.right = right

        self.left._nested = True
        self.right._nested = True

    @immutable
    def for_(self, table):
        self.left = self.left.for_(table)
        self.right = self.right.for_(table)
        return self

    def __str__(self):
        return '{left}{comparator}{right}'.format(
            comparator=self.comparator.value,
            left=str(self.left),
            right=str(self.right),
        )

    def fields(self):
        return self.left.fields() + self.right.fields()


class ContainsCriterion(Criterion):
    def __init__(self, field, container):
        """
        A wrapper for a "IN" criterion.  This wraps two parts, a field and a container.  The field is the part of the
        expression that is checked for membership in the container.  The container can either be a list or a subquery.


        :param field:
            The field to assert membership for within the container.
        :param container:
            A list or subquery.
        """
        self.field = field
        self.container = container

    def __str__(self):
        # FIXME escape
        return "{field} IN ({nested})".format(
            field=str(self.field),
            nested=str(self.container)
        )

    def fields(self):
        return [self.field] + self.field.fields() if self.field.fields else []


class BetweenCriterion(Criterion):
    def __init__(self, field, start, end):
        self.field = field
        self.start = start
        self.end = end

    @immutable
    def for_(self, table):
        self.field = self.field.for_(table)
        return self

    def __str__(self):
        # FIXME escape
        return "{field} BETWEEN {start} AND {end}".format(
            field=self.field,
            start=self.start,
            end=self.end,
        )

    def fields(self):
        return [self.field] + self.field.fields() if self.field.fields else []


class NullCriterion(Criterion):
    def __init__(self, field, isnull):
        self.field = field
        self.isnull = isnull

    @immutable
    def for_(self, table):
        self.field = self.field.for_(table)
        return self

    def __str__(self):
        return "{field} IS{isnot} NULL".format(
            field=self.field,
            isnot='' if self.isnull else ' NOT'
        )

    def fields(self):
        return [self.field] + self.field.fields() if self.field.fields else []


class ComplexCriterion(BasicCriterion):
    def __str__(self):
        return '{left} {comparator} {right}'.format(
            comparator=self.comparator.value,
            left=self._wrap(self.left),
            right=self._wrap(self.right),
        )

    def fields(self):
        return self.left.fields() + self.right.fields()

    def _wrap(self, value):
        if isinstance(value, ComplexCriterion) and not value.comparator == self.comparator:
            return '(%s)' % str(value)

        return str(value)


class ArithmeticFunction(Term):
    def __init__(self, operator, left, right, alias=None):
        """
        Wrapper for an arithmetic expression.

        :param operator:
            Type: Arithmetic
            An operator for the expression such as `+` or `/`

        :param left:
            The term on the left side of the expression.
        :param right:
            The term on the right side of the expression.
        :param alias:
            (Optional) an alias for the term which can be used inside a select statement.
        :return:
        """

        self.operator = operator
        self.left = left
        self.right = right
        self._alias = alias

    @immutable
    def for_(self, table):
        self.left = self.left.for_(table)
        self.right = self.right.for_(table)
        return self

    @immutable
    def as_(self, alias):
        self._alias = alias
        return self

    def fields(self):
        return self.left.fields() + self.right.fields()

    def __str__(self):
        return '{left}{operator}{right}'.format(
            operator=self.operator.value,
            left=str(self.left),
            right=str(self.right),
        )


class Case(Term):
    def __init__(self, alias=None):
        self._cases = []
        self._else = None
        self._alias = alias

    @immutable
    def when(self, criterion, term):
        self._cases.append((criterion, self._wrap(term)))
        return self

    @immutable
    def else_(self, field):
        self._else = self._wrap(field)
        return self

    @immutable
    def as_(self, alias):
        self._alias = alias
        return self

    def __str__(self):
        if not self._cases:
            raise CaseException("At least one 'when' case is required for a CASE statement.")
        if self._else is None:
            raise CaseException("'Else' clause is requred for a CASE statement.")

        return 'CASE {cases} ELSE {else_clause} END'.format(
            cases=" ".join('WHEN {when} THEN {then}'.format(
                when=criterion,
                then=field
            ) for criterion, field in self._cases),
            else_clause=str(self._else),
        )

    def fields(self):
        fields = []

        for criterion, term in self._cases:
            fields += criterion.fields() + term.fields()

        if self._else is not None:
            fields += self._else.fields()

        return fields


class Function(Term):
    def __init__(self, name, *params, **kwargs):
        self.name = name
        self.params = params
        self._alias = kwargs.get('alias')

    @immutable
    def for_(self, table):
        self.params = [param.for_(table) if hasattr(param, 'for_') else param
                       for param in self.params]
        return self

    def __str__(self):
        # FIXME escape
        return '{name}({params})'.format(
            name=self.name,
            params=','.join(map(str, self.params)),
        )

    def fields(self):
        return [field
                for param in self.params
                if hasattr(param, 'fields')
                for field in param.fields()]

    @immutable
    def as_(self, alias):
        self._alias = alias
        return self


class Interval(object):
    units = ['years', 'months', 'days', 'hours', 'minutes', 'seconds', 'microseconds']
    labels = ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'MICROSECOND']

    trim_pattern = re.compile(r'^[0\-\.: ]+|[0\-\.: ]+$')

    def __init__(self, years=0, months=0, days=0, hours=0, minutes=0, seconds=0, microseconds=0, quarters=0, weeks=0):
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
        if hasattr(self, 'quarters'):
            expr = getattr(self, 'quarters')
            unit = 'QUARTER'

        elif hasattr(self, 'weeks'):
            expr = getattr(self, 'weeks')
            unit = 'WEEK'

        else:
            # Create the whole expression but trim out the unnecessery fields
            expr = self.trim_pattern.sub(
                '',
                "{years}-{months}-{days} {hours}:{minutes}:{seconds}.{microseconds}".format(
                    years=getattr(self, 'years', 0),
                    months=getattr(self, 'months', 0),
                    days=getattr(self, 'days', 0),
                    hours=getattr(self, 'hours', 0),
                    minutes=getattr(self, 'minutes', 0),
                    seconds=getattr(self, 'seconds', 0),
                    microseconds=getattr(self, 'microseconds', 0),
                )
            )
            unit = '{largest}_{smallest}'.format(
                largest=self.largest,
                smallest=self.smallest,
            ) if self.largest != self.smallest else self.largest

        return 'INTERVAL {expr} {unit}'.format(
            expr=expr,
            unit=unit,
        )


class Functions(object):
    """
    Package for SQL functions wrappers
    """

    class Count(Function):
        def __init__(self, param, alias=None):
            super(Functions.Count, self).__init__('COUNT', param, alias=alias)
            self.__distinct__ = False

        def __str__(self):
            s = super(Functions.Count, self).__str__()
            if self.__distinct__:
                return s[:6] + 'DISTINCT ' + s[6:]

            return s

        @immutable
        def distinct(self):
            self.__distinct__ = True
            return self

    # Arithmetic Functions
    class Sum(Function):
        def __init__(self, term, alias=None):
            super(Functions.Sum, self).__init__('SUM', term, alias=alias)

    class Avg(Function):
        def __init__(self, term, alias=None):
            super(Functions.Avg, self).__init__('AVG', term, alias=alias)

    class Min(Function):
        def __init__(self, term, alias=None):
            super(Functions.Min, self).__init__('MIN', term, alias=alias)

    class Max(Function):
        def __init__(self, term, alias=None):
            super(Functions.Max, self).__init__('MAX', term, alias=alias)

    class Std(Function):
        def __init__(self, term, alias=None):
            super(Functions.Std, self).__init__('STD', term, alias=alias)

    class StdDev(Function):
        def __init__(self, term, alias=None):
            super(Functions.StdDev, self).__init__('STDDEV', term, alias=alias)

    class Coalesce(Function):
        def __init__(self, term, default_value, alias=None):
            super(Functions.Coalesce, self).__init__('COALESCE', term, self._wrap(default_value), alias=alias)

    class Pow(Function):
        def __init__(self, term, exponent, alias=None):
            super(Functions.Pow, self).__init__('POW', term, exponent, alias=alias)

    class Mod(Function):
        def __init__(self, term, modulus, alias=None):
            super(Functions.Mod, self).__init__('MOD', term, modulus, alias=alias)

    # Type Functions
    class Cast(Function):
        def __init__(self, term, as_type, alias=None):
            super(Functions.Cast, self).__init__('CAST', term, as_type, alias=alias)

        def __str__(self):
            # FIXME escape
            return '{name}({field} AS {type})'.format(
                name=self.name,
                field=self.params[0],
                type=self.params[1],
            )

    class Convert(Function):
        def __init__(self, term, encoding, alias=None):
            super(Functions.Convert, self).__init__('CONVERT', term, encoding, alias=alias)

        def __str__(self):
            # FIXME escape
            return '{name}({field} USING {type})'.format(
                name=self.name,
                field=self.params[0],
                type=self.params[1],
            )

    class Signed(Cast):
        def __init__(self, term, alias=None):
            super(Functions.Signed, self).__init__(self._wrap(term), 'SIGNED', alias=alias)

    class Unsigned(Cast):
        def __init__(self, term, alias=None):
            super(Functions.Unsigned, self).__init__(self._wrap(term), 'UNSIGNED', alias=alias)

    class Date(Function):
        def __init__(self, term, alias=None):
            super(Functions.Date, self).__init__('DATE', self._wrap(term), alias=alias)

    class Timestamp(Function):
        def __init__(self, term, alias=None):
            super(Functions.Timestamp, self).__init__('TIMESTAMP', self._wrap(term), alias=alias)

    # String Functions
    class Ascii(Function):
        def __init__(self, term, alias=None):
            super(Functions.Ascii, self).__init__('ASCII', self._wrap(term), alias=alias)

    class Bin(Function):
        def __init__(self, term, alias=None):
            super(Functions.Bin, self).__init__('BIN', self._wrap(term), alias=alias)

    class Concat(Function):
        def __init__(self, *terms, **kwargs):
            super(Functions.Concat, self).__init__('CONCAT', *[self._wrap(term) for term in terms], **kwargs)

    class Insert(Function):
        def __init__(self, term, start, stop, subterm, alias=None):
            term, start, stop, subterm = [self._wrap(term) for term in [term, start, stop, subterm]]
            super(Functions.Insert, self).__init__('INSERT', term, start, stop, subterm, alias=alias)

    class Length(Function):
        def __init__(self, term, alias=None):
            super(Functions.Length, self).__init__('LENGTH', self._wrap(term), alias=alias)

    class Lower(Function):
        def __init__(self, term, alias=None):
            super(Functions.Lower, self).__init__('LOWER', self._wrap(term), alias=alias)

    # Date Functions
    class Now(Function):
        def __init__(self, alias=None):
            super(Functions.Now, self).__init__('NOW', alias=alias)

    class CurDate(Function):
        def __init__(self, alias=None):
            super(Functions.CurDate, self).__init__('CURDATE', alias=alias)

    class CurTime(Function):
        def __init__(self, alias=None):
            super(Functions.CurTime, self).__init__('CURTIME', alias=alias)

    class Extract(Function):
        def __init__(self, date_part, field, alias=None):
            super(Functions.Extract, self).__init__('EXTRACT', date_part.value, self._wrap(field), alias=alias)

        def __str__(self):
            # FIXME escape
            return '{name}({part} FROM {field})'.format(
                name=self.name,
                part=self.params[0],
                field=self.params[1],
            )
