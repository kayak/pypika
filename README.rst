PyPika - Python Query Builder
=============================

.. _intro_start:

|BuildStatus|  |CoverageStatus|  |Codacy|  |Docs|  |PyPi|  |License|

Abstract
--------

What is |Brand|?

|Brand| is a Python API for building SQL queries. The motivation behind |Brand| is to provide a simple interface for
building SQL queries without limiting the flexibility of handwritten SQL. Designed with data analysis in mind, |Brand|
leverages the builder design pattern to construct queries to avoid messy string formatting and concatenation. It is also
easily extended to take full advantage of specific features of SQL database vendors.

.. _intro_end:

Read the docs: http://pypika.readthedocs.io/en/latest/

Installation
------------

.. _installation_start:

|Brand| supports python ``2.7`` and ``3.3+``.  It may also work on pypy, cython, and jython, but is not being tested for these versions.

To install |Brand| run the following command:

.. code-block:: bash

    pip install pypika


.. _installation_end:


Tutorial
--------

.. _tutorial_start:

The main classes in pypika are ``pypika.Query``, ``pypika.Table``, and ``pypika.Field``.

.. code-block:: python

    from pypika import Query, Table, Field


Selecting Data
^^^^^^^^^^^^^^

The entry point for building queries is ``pypika.Query``.  In order to select columns from a table, the table must
first be added to the query.  For simple queries with only one table, tables and columns can be references using
strings.  For more sophisticated queries a ``pypika.Table`` must be used.

.. code-block:: python

    q = Query.from_('customers').select('id', 'fname', 'lname', 'phone')

To convert the query into raw SQL, it can be cast to a string.

.. code-block:: python

    str(q)

Using ``pypika.Table``

.. code-block:: python

    customers = Table('customers')
    q = Query.from_(customers).select(customers.id, customers.fname, customers.lname, customers.phone)

Both of the above examples result in the following SQL:

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers


Arithmetic
""""""""""

Arithmetic expressions can also be constructed using pypika.  Operators such as `+`, `-`, `*`, and `/` are implemented
by ``pypika.Field`` which can be used simply with a ``pypika.Table`` or directly.

.. code-block:: python

    from pypika import Field

    q = Query.from_('account').select(
        Field('revenue') - Field('cost')
    )

.. code-block:: sql

    SELECT revenue-cost FROM accounts

Using ``pypika.Table``

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
"""""""""

Queries can be filtered with ``pypika.Criterion`` by using equality or inequality operators

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
""""""""""""""""""""""""

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
``Query.having()`` takes a ``Criterion`` parameter similar to the method ``Query.where()``.

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
"""""""""""""""""""""""""""""

Tables and subqueries can be joined to any query using the ``Query.join()`` method.  Joins can be performed with either
a ``USING`` or ``ON`` clauses.  The ``USING`` clause can be used when both tables/subqueries contain the same field and
the ``ON`` clause can be used with a criterion. To perform a join, ``...join()`` can be chained but then must be
followed immediately by ``...on(<criterion>)`` or ``...using(*field)``.

Example of a join using `ON`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

    SELECT "history".* FROM "history" JOIN "customers" ON "history"."customer_id"="customers"."id" WHERE "customers"."id"=5

As a shortcut, the ``Query.join().on_field()`` function is provided for joining the (first) table in the ``FROM`` clause
with the joined table when the field name(s) are the same in both tables.

Example of a join using `ON`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    history, customers = Tables('history', 'customers')
    q = Query.from_(history).join(
        customers
    ).on_field(
        'customer_id', 'group'
    ).select(
        history.star
    ).where(
        customers.group == 'A'
    )


.. code-block:: sql

    SELECT "history".* FROM "history" JOIN "customers" ON "history"."customer_id"="customers"."customer_id" AND "history"."group"="customers"."group" WHERE "customers"."group"='A'


Example of a join using `USING`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    history, customers = Tables('history', 'customers')
    q = Query.from_(history).join(
        customers
    ).on(
        'customer_id'
    ).select(
        history.star
    ).where(
        customers.id == 5
    )


.. code-block:: sql

    SELECT "history".* FROM "history" JOIN "customers" USING "customer_id" WHERE "customers"."id"=5


Unions
""""""

Both ``UNION`` and ``UNION ALL`` are supported. ``UNION DISTINCT`` is synonomous with "UNION`` so and |Brand| does not
provide a separate function for it.  Unions require that queries have the same number of ``SELECT`` clauses so
trying to cast a unioned query to string with through a ``UnionException`` if the column sizes are mismatched.

To create a union query, use either the ``Query.union()`` method or `+` operator with two query instances. For a
union all, use ``Query.union_all()`` or the `*` operator.

.. code-block:: python

    provider_a, provider_b = Tables('provider_a', 'provider_b')
    q = Query.from_(provider_a).select(
        provider_a.created_time, provider_a.foo, provider_a.bar
    ) + Query.from_(provider_b).select(
        provider_b.created_time, provider_b.fiz, provider_b.buz
    )

.. code-block:: sql

    SELECT "created_time","foo","bar" FROM "provider_a" UNION SELECT "created_time","fiz","buz" FROM "provider_b"


Date, Time, and Intervals
"""""""""""""""""""""""""

Using ``pypika.Interval``, queries can be constructed with date arithmetic.  Any combination of intervals can be
used except for weeks and quarters, which must be used separately and will ignore any other values if selected.

.. code-block:: python

    from pypika import functions as fn

    fruits = Tables('fruits')
    q = Query.from_(fruits) \
        .select(fruits.id, fruits.name) \
        .where(fruits.harvest_date + Interval(months=1) < fn.Now())

.. code-block:: sql

    SELECT id,name FROM fruits WHERE harvest_date+INTERVAL 1 MONTH<NOW()


Tuples
""""""

Tuples are supported through the class ``pypika.Tuple`` but also through the native python tuple wherever possible.
Tuples can be used with ``pypika.Criterion`` in **WHERE** clauses for pairwise comparisons.

.. code-block:: python

    from pypika import Query, Tuple

    q = Query.from_(self.table_abc) \
        .select(self.table_abc.foo, self.table_abc.bar) \
        .where(Tuple(self.table_abc.foo, self.table_abc.bar) == Tuple(1, 2))

.. code-block:: sql

    SELECT "foo","bar" FROM "abc" WHERE ("foo","bar")=(1,2)

Using ``pypika.Tuple`` on both sides of the comparison is redundant and |Brand| supports native python tuples.

.. code-block:: python

    from pypika import Query, Tuple

    q = Query.from_(self.table_abc) \
        .select(self.table_abc.foo, self.table_abc.bar) \
        .where(Tuple(self.table_abc.foo, self.table_abc.bar) == (1, 2))

.. code-block:: sql

    SELECT "foo","bar" FROM "abc" WHERE ("foo","bar")=(1,2)

Tuples can be used in **IN** clauses.

.. code-block:: python

    Query.from_(self.table_abc) \
            .select(self.table_abc.foo, self.table_abc.bar) \
            .where(Tuple(self.table_abc.foo, self.table_abc.bar).isin([(1, 1), (2, 2), (3, 3)]))

.. code-block:: sql

    SELECT "foo","bar" FROM "abc" WHERE ("foo","bar") IN ((1,1),(2,2),(3,3))


Strings Functions
"""""""""""""""""

There are several string operations and function wrappers included in |Brand|.  Function wrappers can be found in the
``pypika.functions`` package.  In addition, `LIKE` and `REGEX` queries are supported as well.

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


Case Statements
"""""""""""""""

Case statements allow fow a number of conditions to be checked sequentially and return a value for the first condition
met or otherwise a default value.  The Case object can be used to chain conditions together along with their output
using the ``when`` method and to set the default value using ``else_``.


.. code-block:: python

    from pypika import Case, functions as fn

    customers = Tables('customers')
    q = Query.from_(customers).select(
        customers.id,
        Case()
           .when(customers.fname == "Tom", "It was Tom")
           .when(customers.fname == "John", "It was John")
           .else_("It was someone else.").as_('who_was_it')
    )


.. code-block:: sql

    SELECT "id",CASE WHEN "fname"='Tom' THEN 'It was Tom' WHEN "fname"='John' THEN 'It was John' ELSE 'It was someone else.' END "who_was_it" FROM "customers"


Inserting Data
^^^^^^^^^^^^^^

Data can be inserted into tables either by providing the values in the query or by selecting them through another query.

By default, data can be inserted by providing values for all columns in the order that they are defined in the table.

Insert with values
""""""""""""""""""

.. code-block:: python

    customers = Table('customers')

    q = Query.into(customers).insert(1, 'Jane', 'Doe', 'jane@example.com')

.. code-block:: sql

    INSERT INTO customers VALUES (1,'Jane','Doe','jane@example.com')

Multiple rows of data can be inserted either by chaining the ``insert`` function or passing multiple tuples as args.

.. code-block:: python

    customers = Table('customers')

    q = Query.into(customers).insert(1, 'Jane', 'Doe', 'jane@example.com').insert(2, 'John', 'Doe', 'john@example.com')

.. code-block:: python

    customers = Table('customers')

    q = Query.into(customers).insert((1, 'Jane', 'Doe', 'jane@example.com'),
                                     (2, 'John', 'Doe', 'john@example.com'))

Insert with a SELECT Query
""""""""""""""""""""""""""

.. code-block:: sql

    INSERT INTO customers VALUES (1,'Jane','Doe','jane@example.com'),(2,'John','Doe','john@example.com')


To specify the columns and the order, use the ``columns`` function.

.. code-block:: python

    customers = Table('customers')

    q = Query.into(customers).columns('id', 'fname', 'lname').insert(1, 'Jane', 'Doe')

.. code-block:: sql

    INSERT INTO customers (id,fname,lname) VALUES (1,'Jane','Doe','jane@example.com')


Inserting data with a query works the same as querying data with the additional call to the ``into`` method in the
builder chain.

.. code-block:: python

    customers, customers_backup = Tables('customers', 'customers_backup')

    q = Query.into(customers_backup).from_(customers).select('*')

.. code-block:: sql

    INSERT INTO customers_backup SELECT * FROM customers

.. _tutorial_end:


.. _license_start:


License
-------

Copyright 2016 KAYAK Germany, GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Crafted with ♥ in Berlin.

.. _license_end:


.. _appendix_start:

.. |Brand| replace:: *PyPika*

.. _appendix_end:

.. _available_badges_start:

.. |BuildStatus| image:: https://travis-ci.org/kayak/pypika.svg?branch=master
   :target: https://travis-ci.org/kayak/pypika
.. |CoverageStatus| image:: https://coveralls.io/repos/kayak/pypika/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/kayak/pypika?branch=master
.. |Codacy| image:: https://api.codacy.com/project/badge/Grade/6d7e44e5628b4839a23da0bd82eaafcf
   :target: https://www.codacy.com/app/twheys/pypika
.. |Docs| image:: https://readthedocs.org/projects/pypika/badge/?version=latest
   :target: http://pypika.readthedocs.io/en/latest/
.. |PyPi| image:: https://img.shields.io/pypi/v/pypika.svg?style=flat
   :target: https://pypi.python.org/pypi/pypika
.. |License| image:: https://img.shields.io/hexpm/l/plug.svg?maxAge=2592000
   :target: http://www.apache.org/licenses/LICENSE-2.0

.. _available_badges_end: