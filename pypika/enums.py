# coding: utf8
from aenum import Enum

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class Arithmetic(Enum):
    add = '+'
    sub = '-'
    mul = '*'
    div = '/'


class Comparator(Enum):
    pass


class Equality(Comparator):
    eq = '='
    ne = '<>'
    gt = '>'
    gte = '>='
    lt = '<'
    lte = '<='


class Matching(Comparator):
    like = ' LIKE '
    regex = ' REGEX '
    bin_regex = ' REGEX BINARY '


class Boolean(Comparator):
    and_ = 'AND'
    or_ = 'OR'
    xor_ = 'XOR'


class Order(Enum):
    asc = 'ASC'
    desc = 'DESC'


class JoinType(Enum):
    inner = ''
    left = 'LEFT'
    right = 'RIGHT'
    outer = 'OUTER'


class UnionType(Enum):
    distinct = ''
    all = ' ALL'


class DatePart(Enum):
    year = 'YEAR'
    quarter = 'QUARTER'
    month = 'MONTH'
    week = 'WEEK'
    day = 'DAY'
    hour = 'HOUR'
    minute = 'MINUTE'
    second = 'SECOND'
    microsecond = 'MICROSECOND'


class SqlTypes(Enum):
    SIGNED = 'SIGNED'
    UNSIGNED = 'UNSIGNED'
    utf8 = 'utf8'
    DATE = 'DATE'
    TIMESTAMP = 'TIMESTAMP'
