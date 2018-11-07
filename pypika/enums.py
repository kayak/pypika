# coding: utf-8
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
    not_like = ' NOT LIKE '
    like = ' LIKE '
    not_ilike = ' NOT ILIKE '
    ilike = ' ILIKE '
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
    left_outer = 'LEFT OUTER'
    right_outer = 'RIGHT OUTER'
    full_outer = 'FULL OUTER'
    cross = 'CROSS'


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


class SqlType:
    def __init__(self, name):
        self.name = name

    def __call__(self, length):
        return SqlTypeLength(self.name, length)

    def get_sql(self, **kwargs):
        return '{name}'.format(name=self.name)


class SqlTypeLength:
    def __init__(self, name, length):
        self.name = name
        self.length = length

    def get_sql(self, **kwargs):
        return '{name}({length})'.format(name=self.name,
                                         length=self.length)


class SqlTypes:
    BOOLEAN = 'BOOLEAN'
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    NUMERIC = 'NUMERIC'
    SIGNED = 'SIGNED'
    UNSIGNED = 'UNSIGNED'

    DATE = 'DATE'
    TIME = 'TIME'
    TIMESTAMP = 'TIMESTAMP'

    CHAR = SqlType('CHAR')
    VARCHAR = SqlType('VARCHAR')
    LONG_VARCHAR = SqlType('LONG VARCHAR')
    BINARY = SqlType('BINARY')
    VARBINARY = SqlType('VARBINARY')
    LONG_VARBINARY = SqlType('LONG VARBINARY')



class Dialects(Enum):
    VERTICA = 'vertica'
    CLICKHOUSE = 'clickhouse'
    ORACLE = 'oracle'
    MSSQL = 'mssql'
    MYSQL = 'mysql'
    POSTGRESQL = 'postgressql'
    REDSHIFT = 'redshift'
    SQLLITE = 'sqllite'
