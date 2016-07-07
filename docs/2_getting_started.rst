Getting Started
===============

The main classes in pyqb are :class:`pyqb.Query`, :class:`pyqb.Table`, and :class:`pyqb.Field`.

.. code-block:: python

    from pyqb import Query, Table, Field


Building Queries
----------------

The entry point for building queries is :class:`pyqb.Query`.  In order to select columns from a table, the table must
first be added to the query.  For simple queries with only one table, tables and and columns can be references using
strings.  For more sophisticated queries a :class:`pyqb.Table` must be used.

.. code-block:: python

    q = Query.from_('customers').select('id', 'fname', 'lname', 'phone')

To convert the query into raw SQL, it can be cast to a string.

.. code-block:: python

    str(q)

Using :class:`pyqb.Table`

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(customers.id, customers.fname, customers.lname, customers.phone)

Both of the above examples result in the following SQL:

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers


Arithmetic
----------

Arithmetic expressions can also be constructed using pyqb.  Operators such as `+`, `-`, `*`, and `/` are implemented by
:class:`pyqb.Field` which can be used simply with a :class:`pyqb.Table` or directly.

.. code-block:: python

    from pyqb import Field

    q = Query.from_('account').select(
        Field('revenue') - Field('cost')
    )

.. code-block:: sql

    SELECT revenue-cost FROM accounts

Using :class:`pyqb.Table`

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

Queries can be filtered with :class:`pyqb.Criterion` by using equality or inequality operators

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(
        customers.id, customers.fname, customers.lname, customers.phone
    ).where(
        customers.lname == 'Mustermann'
    )

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers WHERE lname='Mustermann'

Query methods such as select, where, groupby, and orderby can be called multiple times.  Multiple calls to the
where method will add additional conditions as

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

    from pyqb import fn

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

After adding a ``GROUP BY`` clause to a query, the ``HAVING`` clause becomes available.  The method :class:`Query.having()` 
takes a :class:`Criterion` parameter similar to the method :class:`Query.where()`.

.. code-block:: python

    from pyqb import fn

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


Date, Time, and Intervals
-------------------------

Using :class:`pyqb.Interval`, queries can be constructed with date arithmetic.  Any combination of intervals can be used
except for weeks and quarters, which must be used individually.  However, expressions can be chained.

.. code-block:: python

    fruits = Tables('fruits')
    q = Query.from_(fruits).select(
        fruits.id,
        fruits.name,
    ).where(
        fruits.harvest_date + Interval(months=1) < fn.Now()
    )

.. code-block:: sql

    SELECT id,name FROM fruits WHERE harvest_date+INTERVAL 1 MONTH<NOW()


Manipulating Strings
--------------------

WRITEME