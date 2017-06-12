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

GROUP BY Modifiers
------------------

The `ROLLUP` modifier allows for aggregating to higher levels that the given groups, called super-aggregates.

.. code-block:: python

    from pypika import Rollup, functions as fn

    products = Table('products')

    query = Query.from_(products).select(
        products.id,
        products.category,
        fn.Sum(products.price)
    ).rollup(
        products.id,
        products.category
    )

    print(query)

.. code-block:: sql

    SELECT "id","category",SUM("price") FROM "products" GROUP BY ROLLUP("id","category")

