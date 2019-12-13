"""
PyPika is divided into a couple of modules, primarily the ``queries`` and ``terms`` modules.

pypika.queries
--------------

This is where the ``Query`` class can be found which is the core class in PyPika.  Also, other top level classes such
as ``Table`` can be found here.  ``Query`` is a container that holds all of the ``Term`` types together and also
serializes the builder to a string.

pypika.terms
------------

This module contains the classes which represent individual parts of queries that extend the ``Term`` base class.

pypika.functions
----------------

Wrappers for common SQL functions are stored in this package.

pypika.enums
------------

Enumerated values are kept in this package which are used as options for Queries and Terms.


pypika.utils
------------

This contains all of the utility classes such as exceptions and decorators.

"""
from pypika.dialects import (
    ClickHouseQuery,
    Dialects,
    MSSQLQuery,
    MySQLQuery,
    OracleQuery,
    PostgreSQLQuery,
    RedshiftQuery,
    SQLLiteQuery,
    VerticaQuery,
)
from pypika.enums import (
    DatePart,
    JoinType,
    Order,
)
from pypika.queries import (
    AliasedQuery,
    Column,
    Database,
    Query,
    Schema,
    Table,
    make_columns as Columns,
    make_tables as Tables,
)
from pypika.terms import (
    Array,
    Bracket,
    Case,
    Criterion,
    CustomFunction,
    EmptyCriterion,
    Field,
    Index,
    Interval,
    JSON,
    Not,
    NullValue,
    Parameter,
    Rollup,
    Tuple,
)
from pypika.utils import (
    CaseException,
    FunctionException,
    GroupingException,
    JoinException,
    QueryException,
    RollupException,
    UnionException,
)


def __metadata():
    import pkg_resources
    from email import message_from_string

    pkg_info = pkg_resources.get_distribution('pypika')
    metadata = message_from_string(pkg_info.get_metadata('PKG-INFO'))
    return (
        metadata.get('Author'),
        metadata.get('Author-email'),
        pkg_info.version,
    )


(__author__, __email__, __version__,) = __metadata()
