# coding: utf8
from aenum import Enum

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"
__version__ = "0.0.1"


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
    left = ''
    right = 'RIGHT'
    inner = 'INNER'
    outer = 'OUTER'


class UnionType(Enum):
    distinct = ''
    all = ' ALL'
