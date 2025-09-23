Advanced Query Features
=======================

.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:

This section covers the range of functions that are not widely standardized across all SQL databases or meet special
needs.  |Brand| intends to support as many features across different platforms as possible.  If there are any features
specific to a certain platform that PyPika does not support, please create a GitHub issue requesting that it be added.


Handling different database platforms
-------------------------------------

There can sometimes be differences between how database vendors implement SQL in their platform, for example
which quote characters are used. To ensure that the correct SQL standard is used for your platform,
the platform-specific Query classes can be used.

.. code-block:: python

    from pypika import MySQLQuery, MSSQLQuery, PostgreSQLQuery, OracleQuery, VerticaQuery

You can use these query classes as a drop in replacement for the default ``Query`` class shown in the other examples.
Again, if you encounter any issues specific to a platform, please create a GitHub issue on this repository.

Or even different query languages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some services created their own query language similar to SQL. To generate expressions for Jira there is a ``JiraQuery`` class which just returns an instance of ``JiraQueryBuilder()`` so it could be used directly instead.

.. code-block:: python

    from pypika import JiraQuery

    J = JiraQuery.Table()
    query = (
        JiraQuery.where(J.project.isin(["PROJ1", "PROJ2"]))
        .where(J.issuetype == "My issue")
        .where(J.labels.isempty() | J.labels.notin(["stale", "bug"]))
        .where(J.repos.notempty() & J.repos.notin(["main", "dev"]))
    )

.. code-block:: sql

    project IN ("PROJ1","PROJ2") AND issuetype="My issue" AND (labels is EMPTY OR labels NOT IN ("stale","bug")) AND repos is not EMPTY AND repos NOT IN ("main","dev")

GROUP BY Modifiers
------------------

The `ROLLUP` modifier allows for aggregating to higher levels that the given groups, called super-aggregates.

.. code-block:: python

    from pypika import Rollup, functions as fn

    products = Table('products')

    query = Query.from_(products) \
        .select(products.id, products.category, fn.Sum(products.price)) \
        .rollup(products.id, products.category)

.. code-block:: sql

    SELECT "id","category",SUM("price") FROM "products" GROUP BY ROLLUP("id","category")

Pseudo Column
------------------

A pseudo-column is an SQL assigned value (pseudo-field) used in the same context as an column, but not stored on disk.
The pseudo-column can change from database to database, so here it's possible to define them.

.. code-block:: python

    from pypika import Query
    from pypika.terms import PseudoColumn

    CurrentDate = PseudoColumn('current_date')

    Query.from_('products').select(CurrentDate)

.. code-block:: sql

    SELECT current_date FROM "products"

Analytic Queries
----------------

The package ``pypika.analytic`` contains analytic function wrappers.  These can be used in ``SELECT`` clauses when
building queries for databases that support them. Different functions have different arguments but all require some sort
of partitioning.

``NTILE`` and ``RANK``
~~~~~~~~~~~~~~~~~~~~~~

The ``NTILE`` function requires a constant integer argument while the ``RANK`` function takes no arguments.
clause.

.. code-block:: python

    from pypika import Query, Table, analytics as an, functions as fn

    store_sales_fact, date_dimension = Table('store_sales_fact', schema='store'), Table('date_dimension')

    total_sales = fn.Sum(store_sales_fact.sales_quantity).as_('TOTAL_SALES')
    calendar_month_name = date_dimension.calendar_month_name.as_('MONTH')
    ntile = an.NTile(4).order_by(total_sales).as_('NTILE')

    query = Query.from_(store_sales_fact) \
        .join(date_dimension).using('date_key') \
        .select(calendar_month_name, total_sales, ntile) \
        .groupby(calendar_month_name) \
        .orderby(ntile)

.. code-block:: sql

    SELECT "date_dimension"."calendar_month_name" "MONTH",SUM("store_sales_fact"."sales_quantity") "TOTAL_SALES",NTILE(4) OVER(PARTITION BY  ORDER BY SUM("store_sales_fact"."sales_quantity")) "NTILE" FROM "store"."store_sales_fact" JOIN "date_dimension" USING ("date_key") GROUP BY "date_dimension"."calendar_month_name" ORDER BY NTILE(4) OVER(PARTITION BY  ORDER BY SUM("store_sales_fact"."sales_quantity"))

``FIRST_VALUE`` and ``LAST_VALUE``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``FIRST_VALUE`` and ``LAST_VALUE`` both expect a single argument.  They also support an additional ``IGNORE NULLS``
clause.

.. code-block:: python

    from pypika import Query, Table, analytics as an

    t_month = Table('t_month')

    first_month = an.FirstValue(t_month.month) \
        .over(t_month.season) \
        .orderby(t_month.id)

    last_month = an.LastValue(t_month.month) \
        .over(t_month.season) \
        .orderby(t_month.id) \
        .ignore_nulls()

    query = Query.from_(t_month) \
        .select(first_month, last_month)


.. code-block:: sql

    SELECT FIRST_VALUE("month") OVER(PARTITION BY "season" ORDER BY "id"),LAST_VALUE("month" IGNORE NULLS) OVER(PARTITION BY "season" ORDER BY "id") FROM "t_month"

``MEDIAN``, ``AVG`` and ``STDDEV``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These functions take one or more arguments

.. code-block:: python

    from pypika import Query, Table, analytics as an

    customer_dimension = Table('customer_dimension')

    median_income = an.Median(customer_dimension.annual_income).over(customer_dimension.customer_state).as_('MEDIAN')
    avg_income = an.Avg(customer_dimension.annual_income).over(customer_dimension.customer_state).as_('AVG')
    stddev_income = an.StdDev(customer_dimension.annual_income).over(customer_dimension.customer_state).as_('STDDEV')

    query = Query.from_(customer_dimension) \
        .select(median_income, avg_income, stddev_income) \
        .where(customer_dimension.customer_state.isin(['DC', 'WI'])) \
        .orderby(customer_dimension.customer_state)

.. code-block:: sql

    SELECT MEDIAN("annual_income") OVER(PARTITION BY "customer_state") "MEDIAN",AVG("annual_income") OVER(PARTITION BY "customer_state") "AVG",STDDEV("annual_income") OVER(PARTITION BY "customer_state") "STDDEV" FROM "customer_dimension" WHERE "customer_state" IN ('DC','WI') ORDER BY "customer_state"

Window Frames
=============

Functions which use window aggregation expose the functions ``rows()`` and ``range()`` with varying parameters to define
the window.  Both of these functions take one or two parameters which specify the offset boundaries.  Boundaries can be
set either as the current row with ``an.CURRENT_ROW`` or a value preceding or following the current row with
``an.Preceding(constant_value)`` and ``an.Following(constant_value)``.  The ranges can be unbounded preceding or
following the current row by omitting the ``constant_value`` parameter like ``an.Preceding()`` or ``an.Following()``.

``FIRST_VALUE`` and ``LAST_VALUE`` also support window frames.

.. code-block:: python

    from pypika import Query, Table, analytics as an

    t_transactions = Table('t_customers')

    rolling_7_sum = an.Sum(t_transactions.total) \
        .over(t_transactions.item_id) \
        .orderby(t_transactions.day) \
        .rows(an.Preceding(7), an.CURRENT_ROW)

    query = Query.from_(t_transactions) \
        .select(rolling_7_sum)

.. code-block:: sql

    SELECT SUM("total") OVER(PARTITION BY "item_id" ORDER BY "day" ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) FROM "t_customers"

DDL
===

CREATE TABLE
------------

Simple example of creating table:

.. code-block:: python

    from pypika import Query, Table, Columns

    columns = Columns(
        ('id', 'integer'),
        ('price', 'decimal(15, 9)'),
        ('name', 'varchar(128)')
    )

    Query.create_table(Table('items')).columns(*columns).if_not_exists()

.. code-block:: sql

    CREATE TABLE IF NOT EXISTS items (
        id integer,
        price decimal(15, 9),
        name varchar(128)
    )

Create table from query:

.. code-block:: python

    from pypika import Query, Table

    items = Table('items')

    query = Query.select(items.id, items.price, items.name).from_(items)

    Query.create_table(Table('items')).as_select(query)

.. code-block:: sql

    CREATE TABLE items
    AS
        (SELECT id, price, name FROM items)

TEMPORARY and UNLOGGED tables:

.. code-block:: python

    from pypika import Query, Table, Columns

    columns = Columns(
        ('id', 'integer'),
        ('price', 'decimal(15, 9)'),
        ('name', 'varchar(128)')
    )

    Query.create_table(Table('items')).columns(*columns).temporary()
    Query.create_table(Table('items')).columns(*columns).unlogged()

.. code-block:: sql

    CREATE TEMPORARY TABLE items (
        id integer,
        price decimal(15, 9),
        name varchar(128)
    )

    CREATE UNLOGGED TABLE items (
        id integer,
        price decimal(15, 9),
        name varchar(128)
    )

DROP TABLE
----------

.. code-block:: python

    from pypika import Query, Table

    Query.drop_table(Table('items'))
    Query.drop_table(Table('items')).if_exists()

.. code-block:: sql

    DROP TABLE items
    DROP TABLE IF EXISTS items
