Advanced Query Features
=======================

.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:

This section covers the range of functions that are not widely standardized across all SQL databases or meet special
needs.  |Brand| intends to support as many features across different platforms as possible.  If there are any features
specific to a certain platform that PyPika does not support, please create a github issue requesting that it be added.

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