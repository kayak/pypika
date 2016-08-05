Getting Started
===============

The main classes in pypika are :class:`pypika.Query`, :class:`pypika.Table`, and :class:`pypika.Field`.

.. code-block:: python

    from pypika import Query, Table, Field


Building Queries
----------------

The entry point for building queries is :class:`pypika.Query`.  In order to select columns from a table, the table must
first be added to the query.  For simple queries with only one table, tables and and columns can be references using
strings.  For more sophisticated queries a :class:`pypika.Table` must be used.

.. code-block:: python

    q = Query.from_('customers').select('id', 'fname', 'lname', 'phone')

To convert the query into raw SQL, it can be cast to a string.

.. code-block:: python

    str(q)

Using :class:`pypika.Table`

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(customers.id, customers.fname, customers.lname, customers.phone)

Both of the above examples result in the following SQL:

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers


Arithmetic
----------

Arithmetic expressions can also be constructed using pypika.  Operators such as `+`, `-`, `*`, and `/` are implemented
by :class:`pypika.Field` which can be used simply with a :class:`pypika.Table` or directly.

.. code-block:: python

    from pypika import Field

    q = Query.from_('account').select(
        Field('revenue') - Field('cost')
    )

.. code-block:: sql

    SELECT revenue-cost FROM accounts

Using :class:`pypika.Table`

.. code-block:: python

    accounts = Table('accounts')
    q = Query.from_(accounts).select(
        accounts.revenue - accounts.cost
    )

.. code-block:: sql

    SELECT revenue-cost FROM accounts

An alias can also be used for fields and expressions.

.. code-block:: sql

    q = Query.from_(accounts).select(
        (accounts.revenue - accounts.cost).as_('profit')
    )

.. code-block:: sql

    SELECT revenue-cost profit FROM accounts

More arithmetic examples

.. code-block:: python

    table = Table('table')
    q = Query.from_(table).select(
        table.foo + table.bar,
        table.foo - table.bar,
        table.foo * table.bar,
        table.foo / table.bar,
        (table.foo+table.bar) / table.fiz,
    )

.. code-block:: sql

    SELECT foo+bar,foo-bar,foo*bar,foo/bar,(foo+bar)/fiz FROM table


Filtering
---------

Queries can be filtered with :class:`pypika.Criterion` by using equality or inequality operators

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(
        customers.id, customers.fname, customers.lname, customers.phone
    ).where(
        customers.lname == 'Mustermann'
    )

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers WHERE lname='Mustermann'

Query methods such as select, where, groupby, and orderby can be called multiple times.  Multiple calls to the where
method will add additional conditions as

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(
        customers.id, customers.fname, customers.lname, customers.phone
    ).where(
        customers.fname == 'Max'
    ).where(
        customers.lname == 'Mustermann'
    )

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers WHERE fname='Max' AND lname='Mustermann'

Filters such as IN and BETWEEN are also supported

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(
        customers.id,customers.fname
    ).where(
        customers.age[18:65] & customers.status.isin(['new', 'active'])
    )

.. code-block:: sql

    SELECT id,fname FROM customers WHERE age BETWEEN 18 AND 65 AND status IN ('new','active')

Filtering with complex criteria can be created using boolean symbols ``&``, ``|``, and ``^``.

AND

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(
        customers.id, customers.fname, customers.lname, customers.phone
    ).where(
        (customers.age >= 18) & (customers.lname == 'Mustermann')
    )

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers WHERE age>=18 AND lname='Mustermann'

OR

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(
        customers.id, customers.fname, customers.lname, customers.phone
    ).where(
        (customers.age >= 18) | (customers.lname == 'Mustermann')
    )

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers WHERE age>=18 OR lname='Mustermann'

XOR

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(
        customers.id, customers.fname, customers.lname, customers.phone
    ).where(
        (customers.age >= 18) ^ customers.is_registered
    )

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers WHERE age>=18 XOR is_registered


Grouping and Aggregating
------------------------

Grouping allows for aggregated results and works similar to ``SELECT`` clauses.

.. code-block:: python

    from pypika import functions as fn

    customers = Table('customers')
    q = Query.from_(customers).where(
        customers.age >= 18
    ).groupby(
        customers.id
    ).select(
        customers.id, fn.Sum(customers.revenue)
    )

.. code-block:: sql

    SELECT id,SUM(revenue) FROM customers WHERE age>=18 GROUP BY id ORDER BY id ASC

After adding a ``GROUP BY`` clause to a query, the ``HAVING`` clause becomes available.  The method
:class:`Query.having()` takes a :class:`Criterion` parameter similar to the method :class:`Query.where()`.

.. code-block:: python

    from pypika import functions as fn

    payments = Table('payments')
    q = Query.from_(payments).where(
        payments.transacted[date(2015, 1, 1):date(2016, 1, 1)]
    ).groupby(
        payments.customer_id
    ).having(
        fn.Sum(payments.total) >= 1000
    ).select(
        payments.customer_id, fn.Sum(payments.total)
    )

.. code-block:: sql

    SELECT customer_id,SUM(total) FROM payments 
    WHERE transacted BETWEEN '2015-01-01' AND '2016-01-01' 
    GROUP BY customer_id HAVING SUM(total)>=1000


Joining Tables and Subqueries
-----------------------------

Tables and subqueries can be joined to any query using the :class:`Query.join()` method.  When joining tables and
subqueries, a criterion must provided containing an equality between a field from the primary table or joined tables and
a field from the joining table.  When calling :class:`Query.join()` with a table, a :class:`TablerJoiner` will be
returned with only the :class:`Joiner.on()` function available which takes a :class:`Criterion` parameter.  After
calling :class:`Joiner.on()` the original query builder is returned and additional methods may be chained.

.. code-block:: python

    history, customers = Tables('history', 'customers')
    q = Query.from_(history).join(
        customers
    ).on(
        history.customer_id == customers.id
    ).select(
        history.star
    ).where(
        customers.id == 5
    )

.. code-block:: sql

    SELECT t0.* FROM history t0 JOIN customers t1 ON t0.customer_id=t1.id WHERE t1.id=5

Unions
------

Both ``UNION`` and ``UNION ALL`` are supported. ``UNION DISTINCT`` is synonomous with "UNION`` so and the **pypika**
does not provide a separate function for it.  Unions require that queries have the same number of ``SELECT`` clauses so
trying to cast a unioned query to string with through a :class:`UnionException` if the column sizes are mismatched.

To create a union query, use either the :class:`Query.union()` method or `+` operator with two query instances. For a
union all, use :class:`Query.union_all()` or the `*` operator.

.. code-block:: python

    provider_a, provider_b = Tables('provider_a', 'provider_b')
    q = Query.from_(provider_a).select(
        provider_a.created_time, provider_a.foo, provider_a.bar
    ) + Query.from_(provider_b).select(
        provider_b.created_time, provider_b.fiz, provider_b.buz
    )

.. code-block:: sql

    SELECT created_time,foo,bar FROM provider_a UNION SELECT created_time,fiz,buz FROM provider_b


Date, Time, and Intervals
-------------------------

Using :class:`pypika.Interval`, queries can be constructed with date arithmetic.  Any combination of intervals can be
used except for weeks and quarters, which must be used separately and will ignore any other values if selected.

.. code-block:: python

    from pypika import functions as fn

    fruits = Tables('fruits')
    q = Query.from_(fruits).select(
        fruits.id,
        fruits.name,
    ).where(
        fruits.harvest_date + Interval(months=1) < fn.Now()
    )

.. code-block:: sql

    SELECT id,name FROM fruits WHERE harvest_date+INTERVAL 1 MONTH<NOW()


Strings Functions
-----------------

There are several string operations and function wrappers included in *PyPika*.  Function wrappers can be found in the
:class:`pypika.functions` package.  In addition, `LIKE` and `REGEX` queries are supported as well.

.. code-block:: python

    from pypika import functions as fn

    customers = Tables('customers')
    q = Query.from_(customers).select(
        customers.id,
        customers.fname,
        customers.lname,
    ).where(
        customers.lname.like('Mc%')
    )

.. code-block:: sql

    SELECT id,fname,lname FROM customers WHERE lname LIKE 'Mc%'

.. code-block:: python

    from pypika import functions as fn

    customers = Tables('customers')
    q = Query.from_(customers).select(
        customers.id,
        customers.fname,
        customers.lname,
    ).where(
        customers.lname.regex(r'^[abc][a-zA-Z]+&')
    )

.. code-block:: sql

    SELECT id,fname,lname FROM customers WHERE lname REGEX '^[abc][a-zA-Z]+&';


.. code-block:: python

    from pypika import functions as fn

    customers = Tables('customers')
    q = Query.from_(customers).select(
        customers.id,
        fn.Concat(customers.fname, ' ', customers.lname).as_('full_name'),
    )

.. code-block:: sql

    SELECT id,CONCAT(fname, ' ', lname) full_name FROM customers


Inserting Data
--------------

WRITEME