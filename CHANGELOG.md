# Changelog

## v0.49.0 (2025-01-13)

### New Features

- **JQL (Jira Query Language) support** - New `JiraQuery` class for building Jira queries with `isempty()` and `notempty()` methods (#721)
- **QUALIFY clause** - Filter rows based on window function results (#841)
- **Bitwise OR support** - New `bitwiseor()` method on fields (#825)
- **ClickHouse DISTINCT ON** - `distinct_on()` method for ClickHouse queries (#817)
- **ClickHouse LIMIT BY** - `limit_by()` and `limit_offset_by()` methods (#817)
- **ClickHouse FINAL** - `final()` method for FINAL keyword (#765)
- **ClickHouse SAMPLE** - `sample()` method for approximate query processing (#707)
- **Oracle LIMIT/OFFSET** - `limit()` and `offset()` support using FETCH NEXT/OFFSET ROWS syntax (#754)
- **CREATE/DROP INDEX** - `Query.create_index()` and `Query.drop_index()` with support for unique, partial, and conditional indices (#753)
- **Pipe operator** - `QueryBuilder.pipe()` method for functional composition (#759)
- **Improved parameterized queries** - `get_parameters()` method to collect parameter values, `ListParameter` and `DictParameter` base classes (#794)
- **Auto increment columns** - Auto increment support on SqlTypes for CREATE TABLE (#829)
- **datetime.time support** - Proper handling of `datetime.time` values (#837)

### Bug Fixes

- Fixed `EmptyCriterion` handling in `&`, `|`, `^` operations with `Criterion` (#732)
- Fixed `Field.__init__` to accept string table names (#742)
- Fixed `isin()` and `notin()` to accept `frozenset` (#744)
- Fixed MySQL set operations (UNION/INTERSECT/MINUS) to wrap queries in parentheses (#782)
- Fixed PostgreSQL array syntax for UPDATE statements (#644)
- Fixed escape quotes in identifiers (#811)

### Typing & Compatibility

- Added `py.typed` marker for PEP 561 compliance (#666)
- Fixed typing hint for `@builder` decorator to preserve callable type (#740)
- Fixed types for mypy downstream usage, added `__all__` exports (#815)
- Improved typing for builder methods (#850)
- Added Python 3.13 and 3.14 support (#848, #851)
- Applied future type style annotations (#853)

### Documentation

- Added comprehensive documentation for all new features to README.rst
- Added documentation for: Analytic Queries (NTILE, RANK, FIRST_VALUE, LAST_VALUE, MEDIAN, AVG, STDDEV), Window Frames, ROLLUP, Pseudo Columns, TEMPORARY/UNLOGGED tables
- Unified documentation between README.rst and ReadTheDocs (single source of truth)
- Fixed PseudoColumn import in docs (#698)
- Added contributing guidelines (#758)
- Various typo fixes (#695, #801, #804)

### Internal

- Refactored `Interval` to inherit from `Term` (#838)
- Refactored shared logic for `Criterion` comparisons (#849)
- Updated CI to test Python 3.8-3.14 and PyPy 3.9-3.11 on Ubuntu and macOS
- Added GitHub Actions release workflow for PyPI publishing
- Removed tox dependency, simplified test running
- Updated Sphinx and other dev dependencies
