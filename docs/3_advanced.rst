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

