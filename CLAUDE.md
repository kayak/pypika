# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PyPika is a Python SQL query builder library that uses a builder design pattern to construct SQL queries programmatically. It supports 10+ database dialects (MySQL, PostgreSQL, Oracle, MSSQL, Vertica, ClickHouse, Snowflake, SQLite, Redshift, JQL) with no external dependencies.

## Development Commands

```bash
# Install dev dependencies and pre-commit hooks
make install

# Run all tests
python -m unittest discover

# Run a single test file
python -m unittest pypika.tests.test_criterions

# Run a specific test class or method
python -m unittest pypika.tests.test_criterions.CriterionTests.test_empty_criterion

# Code formatting (Black, line length: 120)
black pypika/
black --check pypika/

# Build documentation
make docs.build
```

## Architecture

The codebase follows a layered builder pattern:

### Core Modules

- **`queries.py`** - Main query builder classes: `Query`, `QueryBuilder`, `CreateQueryBuilder`, `DropQueryBuilder`, table/schema/join management
- **`terms.py`** - Term hierarchy: `Term`, `Field`, `Criterion`, `Function`, `ArithmeticExpression`, `Case`, `ValueWrapper`, `Parameter`
- **`dialects.py`** - Database-specific query builders that override base behavior for MySQL, PostgreSQL, Oracle, MSSQL, etc.
- **`functions.py`** - SQL function wrappers (aggregates, string, date/time, math, casting)
- **`analytics.py`** - Window/analytic functions: `Rank`, `DenseRank`, `RowNumber`, `LAG`, `LEAD`
- **`enums.py`** - Enumerations: `JoinType`, `Order`, `Comparator`, `Arithmetic`, `DatePart`, `Dialects`
- **`utils.py`** - Exceptions and decorators (`@builder`, `@ignore_copy`)

### Key Design Patterns

1. **Builder Pattern with Immutability**: The `@builder` decorator in `utils.py` creates deep copies on each method call, enabling safe method chaining.

2. **Visitor Pattern**: Every term/query class implements `get_sql()` to serialize to SQL strings recursively.

3. **Operator Overloading**: `Criterion` and `Field` classes overload operators for readable query conditions:
   ```python
   field == value     # BasicCriterion with Equality.eq
   field[18:65]       # BetweenCriterion
   crit1 & crit2      # ComplexCriterion with AND
   ```

### Class Hierarchy

```
Term (base for query components)
├── Field → extends Criterion (column references)
├── Criterion (boolean conditions)
│   ├── BasicCriterion, ContainsCriterion, BetweenCriterion
│   └── ComplexCriterion (nested AND/OR)
├── Function → AggregateFunction → AnalyticFunction
├── ArithmeticExpression, Case, ValueWrapper, Parameter
└── Array, Tuple, Bracket, Interval

QueryBuilder (fluent builder for SELECT)
└── [dialect-specific variants in dialects.py]

Query (entry point, creates QueryBuilder instances)
```

### Query Entry Points

```python
Query.from_(table).select(...).where(...).orderby(...)
Query.into(table).columns(...).insert(...)
Query.update(table).set(...).where(...)
Query.delete().from_(table).where(...)
Query.create_table(name).columns(...)
```

## Testing

Tests are in `pypika/tests/` using unittest. Key test files:
- `test_criterions.py` - Filtering/conditions
- `test_joins.py` - JOIN operations
- `test_functions.py` - SQL functions
- `test_inserts.py`, `test_updates.py`, `test_deletes.py` - DML operations
- `dialects/` - Dialect-specific tests
- `clickhouse/` - ClickHouse function tests

## Code Style

- Black formatter with 120-char line length
- No external dependencies in core library
- Public API exported via `pypika/__init__.py` with explicit `__all__`

## Documentation

Documentation is published to three places from a single source:

```
README.rst (single source of truth)
    │
    ├──► PyPI (setup.py reads README.rst as long_description)
    ├──► GitHub (renders README.rst directly)
    └──► ReadTheDocs (docs/*.rst files include sections from README.rst)
```

### Structure

- **`README.rst`** - Main documentation file with all content. Uses RST markers like `_tutorial_start:` and `_tutorial_end:` to define includable sections.
- **`docs/*.rst`** - Sphinx source files that `.. include::` sections from README.rst using markers.
- **`docs/conf.py`** - Sphinx configuration.

### Key Markers in README.rst

```rst
.. _intro_start:        → docs/index.rst
.. _intro_end:

.. _installation_start: → docs/1_installation.rst
.. _installation_end:

.. _tutorial_start:     → docs/2_tutorial.rst (main content)
.. _tutorial_end:

.. _advanced_start:     → docs/3_advanced.rst
.. _advanced_end:
```

### Adding Documentation

1. Add new content to `README.rst` within the appropriate markers
2. Content automatically appears on PyPI, GitHub, and ReadTheDocs
3. Run `make docs.build` to verify Sphinx builds correctly
