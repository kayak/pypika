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

What are the design goals for |Brand|?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

|Brand| is a fast, expressive and flexible way to replace handwritten SQL (or even ORM for the courageous souls amongst you).
Validation of SQL correctness is not an explicit goal of |Brand|. With such a large number of
SQL database vendors providing a robust validation of input data is difficult. Instead you are encouraged to check inputs you provide to |Brand| or appropriately handle errors raised from
your SQL database - just as you would have if you were writing SQL yourself.

.. _intro_end:

Read the docs: http://pypika.readthedocs.io/en/latest/

Installation
------------

.. _installation_start:

|Brand| supports python ``3.6+``.  It may also work on pypy, cython, and jython, but is not being tested for these versions.

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

Alternatively, you can use the `Query.get_sql()` function:

.. code-block:: python

    q.get_sql()


Tables, Columns, Schemas, and Databases
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In simple queries like the above example, columns in the "from" table can be referenced by passing string names into
the ``select`` query builder function. In more complex examples, the ``pypika.Table`` class should be used. Columns can be
referenced as attributes on instances of ``pypika.Table``.

.. code-block:: python

    from pypika import Table, Query

    customers = Table('customers')
    q = Query.from_(customers).select(customers.id, customers.fname, customers.lname, customers.phone)

Both of the above examples result in the following SQL:

.. code-block:: sql

    SELECT id,fname,lname,phone FROM customers

An alias for the table can be given using the ``.as_`` function on ``pypika.Table``

.. code-block:: sql

    Table('x_view_customers').as_('customers')
    q = Query.from_(customers).select(customers.id, customers.phone)

.. code-block:: sql

    SELECT id,phone FROM x_view_customers customers

A schema can also be specified. Tables can be referenced as attributes on the schema.

.. code-block:: sql

    from pypika import Table, Query, Schema

    views = Schema('views')
    q = Query.from_(views.customers).select(customers.id, customers.phone)

.. code-block:: sql

    SELECT id,phone FROM views.customers

Also references to databases can be used. Schemas can be referenced as attributes on the database.

.. code-block:: sql

    from pypika import Table, Query, Database

    my_db = Database('my_db')
    q = Query.from_(my_db.analytics.customers).select(customers.id, customers.phone)

.. code-block:: sql

    SELECT id,phone FROM my_db.analytics.customers


Results can be ordered by using the following syntax:

.. code-block:: python

    from pypika import Order
    Query.from_('customers').select('id', 'fname', 'lname', 'phone').orderby('id', order=Order.desc)

This results in the following SQL:

.. code-block:: sql

    SELECT "id","fname","lname","phone" FROM "customers" ORDER BY "id" DESC

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


Convenience Methods
"""""""""""""""""""

In the `Criterion` class, there are the static methods `any` and `all` that allow building chains AND and OR expressions with a list of terms.

.. code-block:: python

    from pypika import Criterion

    customers = Table('customers')
    q = Query.from_(customers).select(
        customers.id,
        customers.fname
    ).where(
        Criterion.all([
            customers.is_registered,
            customers.age >= 18,
            customers.lname == "Jones",
        ])
    )

.. code-block:: sql

    SELECT id,fname FROM customers WHERE is_registered AND age>=18 AND lname = "Jones"


Grouping and Aggregating
""""""""""""""""""""""""

Grouping allows for aggregated results and works similar to ``SELECT`` clauses.

.. code-block:: python

    from pypika import functions as fn

    customers = Table('customers')
    q = Query \
        .from_(customers) \
        .where(customers.age >= 18) \
        .groupby(customers.id) \
        .select(customers.id, fn.Sum(customers.revenue))

.. code-block:: sql

    SELECT id,SUM("revenue") FROM "customers" WHERE "age">=18 GROUP BY "id"

After adding a ``GROUP BY`` clause to a query, the ``HAVING`` clause becomes available.  The method
``Query.having()`` takes a ``Criterion`` parameter similar to the method ``Query.where()``.

.. code-block:: python

    from pypika import functions as fn

    payments = Table('payments')
    q = Query \
        .from_(payments) \
        .where(payments.transacted[date(2015, 1, 1):date(2016, 1, 1)]) \
        .groupby(payments.customer_id) \
        .having(fn.Sum(payments.total) >= 1000) \
        .select(payments.customer_id, fn.Sum(payments.total))

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


Join Types
~~~~~~~~~~

All join types are supported by |Brand|.

.. code-block:: python

    Query \
        .from_(base_table)
        ...
        .join(join_table, JoinType.left)
        ...


.. code-block:: python

    Query \
        .from_(base_table)
        ...
        .left_join(join_table) \
        .left_outer_join(join_table) \
        .right_join(join_table) \
        .right_outer_join(join_table) \
        .inner_join(join_table) \
        .outer_join(join_table) \
        .full_outer_join(join_table) \
        .cross_join(join_table) \
        .hash_join(join_table) \
        ...

See the list of join types here ``pypika.enums.JoinTypes``

Example of a join using `ON`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    history, customers = Tables('history', 'customers')
    q = Query \
        .from_(history) \
        .join(customers) \
        .on(history.customer_id == customers.id) \
        .select(history.star) \
        .where(customers.id == 5)


.. code-block:: sql

    SELECT "history".* FROM "history" JOIN "customers" ON "history"."customer_id"="customers"."id" WHERE "customers"."id"=5

As a shortcut, the ``Query.join().on_field()`` function is provided for joining the (first) table in the ``FROM`` clause
with the joined table when the field name(s) are the same in both tables.

Example of a join using `ON`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    history, customers = Tables('history', 'customers')
    q = Query \
        .from_(history) \
        .join(customers) \
        .on_field('customer_id', 'group') \
        .select(history.star) \
        .where(customers.group == 'A')


.. code-block:: sql

    SELECT "history".* FROM "history" JOIN "customers" ON "history"."customer_id"="customers"."customer_id" AND "history"."group"="customers"."group" WHERE "customers"."group"='A'


Example of a join using `USING`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    history, customers = Tables('history', 'customers')
    q = Query \
        .from_(history) \
        .join(customers) \
        .using('customer_id') \
        .select(history.star) \
        .where(customers.id == 5)


.. code-block:: sql

    SELECT "history".* FROM "history" JOIN "customers" USING "customer_id" WHERE "customers"."id"=5


Example of a correlated subquery in the `SELECT`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    history, customers = Tables('history', 'customers')
    last_purchase_at = Query.from_(history).select(
        history.purchase_at
    ).where(history.customer_id==customers.customer_id).orderby(
        history.purchase_at, order=Order.desc
    ).limit(1)
    q = Query.from_(customers).select(
        customers.id, last_purchase_at.as_('last_purchase_at')
    )


.. code-block:: sql

    SELECT
      "id",
      (SELECT "history"."purchase_at"
       FROM "history"
       WHERE "history"."customer_id" = "customers"."customer_id"
       ORDER BY "history"."purchase_at" DESC
       LIMIT 1) "last_purchase_at"
    FROM "customers"


Unions
""""""

Both ``UNION`` and ``UNION ALL`` are supported. ``UNION DISTINCT`` is synonomous with "UNION`` so and |Brand| does not
provide a separate function for it.  Unions require that queries have the same number of ``SELECT`` clauses so
trying to cast a unioned query to string with through a ``SetOperationException`` if the column sizes are mismatched.

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

Intersect
"""""""""

``INTERSECT`` is supported. Intersects require that queries have the same number of ``SELECT`` clauses so
trying to cast a intersected query to string with through a ``SetOperationException`` if the column sizes are mismatched.

To create a intersect query, use the ``Query.intersect()`` method.

.. code-block:: python

    provider_a, provider_b = Tables('provider_a', 'provider_b')
    q = Query.from_(provider_a).select(
        provider_a.created_time, provider_a.foo, provider_a.bar
    )
    r = Query.from_(provider_b).select(
        provider_b.created_time, provider_b.fiz, provider_b.buz
    )
    intersected_query = q.intersect(r)

.. code-block:: sql

    SELECT "created_time","foo","bar" FROM "provider_a" INTERSECT SELECT "created_time","fiz","buz" FROM "provider_b"

Minus
"""""

``MINUS`` is supported. Minus require that queries have the same number of ``SELECT`` clauses so
trying to cast a minus query to string with through a ``SetOperationException`` if the column sizes are mismatched.

To create a minus query, use either the ``Query.minus()`` method or `-` operator with two query instances.

.. code-block:: python

    provider_a, provider_b = Tables('provider_a', 'provider_b')
    q = Query.from_(provider_a).select(
        provider_a.created_time, provider_a.foo, provider_a.bar
    )
    r = Query.from_(provider_b).select(
        provider_b.created_time, provider_b.fiz, provider_b.buz
    )
    minus_query = q.minus(r)

    (or)

    minus_query = Query.from_(provider_a).select(
        provider_a.created_time, provider_a.foo, provider_a.bar
    ) - Query.from_(provider_b).select(
        provider_b.created_time, provider_b.fiz, provider_b.buz
    )

.. code-block:: sql

    SELECT "created_time","foo","bar" FROM "provider_a" MINUS SELECT "created_time","fiz","buz" FROM "provider_b"

EXCEPT
""""""

``EXCEPT`` is supported. Minus require that queries have the same number of ``SELECT`` clauses so
trying to cast a except query to string with through a ``SetOperationException`` if the column sizes are mismatched.

To create a except query, use the ``Query.except_of()`` method.

.. code-block:: python

    provider_a, provider_b = Tables('provider_a', 'provider_b')
    q = Query.from_(provider_a).select(
        provider_a.created_time, provider_a.foo, provider_a.bar
    )
    r = Query.from_(provider_b).select(
        provider_b.created_time, provider_b.fiz, provider_b.buz
    )
    minus_query = q.except_of(r)

.. code-block:: sql

    SELECT "created_time","foo","bar" FROM "provider_a" EXCEPT SELECT "created_time","fiz","buz" FROM "provider_b"

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


Custom Functions
""""""""""""""""

Custom Functions allows us to use any function on queries, as some functions are not covered by PyPika as default, we can appeal
to Custom functions.

.. code-block:: python

    from pypika import CustomFunction

    customers = Tables('customers')
    DateDiff = CustomFunction('DATE_DIFF', ['interval', 'start_date', 'end_date'])

    q = Query.from_(customers).select(
        customers.id,
        customers.fname,
        customers.lname,
        DateDiff('day', customers.created_date, customers.updated_date)
    )

.. code-block:: sql

    SELECT id,fname,lname,DATE_DIFF('day',created_date,updated_date) FROM customers

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


With Clause
"""""""""""""""

With clause allows give a sub-query block a name, which can be referenced in several places within the main SQL query.
The SQL WITH clause is basically a drop-in replacement to the normal sub-query.

.. code-block:: python

    from pypika import Table, AliasedQuery, Query

    customers = Table('customers')

    sub_query = (Query
                .from_(customers)
                .select('*'))

    test_query = (Query
                .with_(sub_query, "an_alias")
                .from_(AliasedQuery("an_alias"))
                .select('*'))

You can use as much as `.with_()` as you want.

.. code-block:: sql

    WITH an_alias AS (SELECT * FROM "customers") SELECT * FROM an_alias


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

.. code-block:: python

    customers =  Table('customers')

    q = customers.insert(1, 'Jane', 'Doe', 'jane@example.com')

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

Insert with constraint violation handling
"""""""""""""""""""""""""""""""""""""""""

MySQL
~~~~~

.. code-block:: python

    customers = Table('customers')

    q = Query.into(customers)\
        .insert(1, 'Jane', 'Doe', 'jane@example.com')\
        .on_duplicate_key_ignore())

.. code-block:: sql

    INSERT INTO customers VALUES (1,'Jane','Doe','jane@example.com') ON DUPLICATE KEY IGNORE

.. code-block:: python

    customers = Table('customers')

    q = Query.into(customers)\
        .insert(1, 'Jane', 'Doe', 'jane@example.com')\
        .on_duplicate_key_update(customers.email, Values(customers.email))

.. code-block:: sql

    INSERT INTO customers VALUES (1,'Jane','Doe','jane@example.com') ON DUPLICATE KEY UPDATE `email`=VALUES(`email`)

``.on_duplicate_key_update`` works similar to ``.set`` for updating rows, additionally it provides the ``Values``
wrapper to update to the value specified in the ``INSERT`` clause.

PostgreSQL
~~~~~~~~~~

.. code-block:: python

    customers = Table('customers')

    q = Query.into(customers)\
        .insert(1, 'Jane', 'Doe', 'jane@example.com')\
        .on_conflict(customers.email)
        .do_nothing()

.. code-block:: sql

    INSERT INTO "abc" VALUES (1,'Jane','Doe','jane@example.com') ON CONFLICT ("email") DO NOTHING

.. code-block:: python

    customers = Table('customers')

    q = Query.into(customers)\
        .insert(1, 'Jane', 'Doe', 'jane@example.com')\
        .on_conflict(customers.email)
        .do_update(customers.email, 'bob@example.com')

.. code-block:: sql

    INSERT INTO "customers" VALUES (1,'Jane','Doe','jane@example.com') ON CONFLICT ("email") DO UPDATE SET "email"='bob@example.com'


Insert from a SELECT Sub-query
""""""""""""""""""""""""""""""

.. code-block:: sql

    INSERT INTO "customers" VALUES (1,'Jane','Doe','jane@example.com'),(2,'John','Doe','john@example.com')


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

.. code-block:: python

    customers, customers_backup = Tables('customers', 'customers_backup')

    q = Query.into(customers_backup).columns('id', 'fname', 'lname')
        .from_(customers).select(customers.id, customers.fname, customers.lname)

.. code-block:: sql

    INSERT INTO customers_backup SELECT "id", "fname", "lname" FROM customers

The syntax for joining tables is the same as when selecting data

.. code-block:: python

    customers, orders, orders_backup = Tables('customers', 'orders', 'orders_backup')

    q = Query.into(orders_backup).columns('id', 'address', 'customer_fname', 'customer_lname')
        .from_(customers)
        .join(orders).on(orders.customer_id == customers.id)
        .select(orders.id, customers.fname, customers.lname)

.. code-block:: sql

   INSERT INTO "orders_backup" ("id","address","customer_fname","customer_lname")
   SELECT "orders"."id","customers"."fname","customers"."lname" FROM "customers"
   JOIN "orders" ON "orders"."customer_id"="customers"."id"

Updating Data
^^^^^^^^^^^^^^
PyPika allows update queries to be constructed with or without where clauses.

.. code-block:: python

    customers = Table('customers')

    Query.update(customers).set(customers.last_login, '2017-01-01 10:00:00')

    Query.update(customers).set(customers.lname, 'smith').where(customers.id == 10)

.. code-block:: sql

    UPDATE "customers" SET "last_login"='2017-01-01 10:00:00'

    UPDATE "customers" SET "lname"='smith' WHERE "id"=10

The syntax for joining tables is the same as when selecting data

.. code-block:: python

    customers, profiles = Tables('customers', 'profiles')

    Query.update(customers)
         .join(profiles).on(profiles.customer_id == customers.id)
         .set(customers.lname, profiles.lname)

.. code-block:: sql

   UPDATE "customers"
   JOIN "profiles" ON "profiles"."customer_id"="customers"."id"
   SET "customers"."lname"="profiles"."lname"

Using ``pypika.Table`` alias to perform the update

.. code-block:: python

    customers = Table('customers')

    customers.update()
            .set(customers.lname, 'smith')
            .where(customers.id == 10)

.. code-block:: sql

    UPDATE "customers" SET "lname"='smith' WHERE "id"=10

Using ``limit`` for performing update

.. code-block:: python

    customers = Table('customers')

    customers.update()
            .set(customers.lname, 'smith')
            .limit(2)

.. code-block:: sql

    UPDATE "customers" SET "lname"='smith' LIMIT 2


Parametrized Queries
^^^^^^^^^^^^^^^^^^^^

PyPika allows you to use ``Parameter(str)`` term as a placeholder for parametrized queries.

.. code-block:: python

    customers = Table('customers')

    q = Query.into(customers).columns('id', 'fname', 'lname')
        .insert(Parameter(':1'), Parameter(':2'), Parameter(':3'))

.. code-block:: sql

    INSERT INTO customers (id,fname,lname) VALUES (:1,:2,:3)

This allows you to build prepared statements, and/or avoid SQL-injection related risks.

Due to the mix of syntax for parameters, depending on connector/driver, it is required that you specify the
parameter token explicitly or use one of the specialized Parameter types per [PEP-0249](https://www.python.org/dev/peps/pep-0249/#paramstyle):
``QmarkParameter()``, ``NumericParameter(int)``,  ``NamedParameter(str)``, ``FormatParameter()``, ``PyformatParameter(str)``

An example of some common SQL parameter styles used in Python drivers are:

PostgreSQL:
    ``$number`` OR ``%s`` + ``:name`` (depending on driver)
MySQL:
    ``%s``
SQLite:
    ``?``
Vertica:
    ``:name``
Oracle:
    ``:number`` + ``:name``
MSSQL:
    ``%(name)s`` OR ``:name`` + ``:number`` (depending on driver)

You can find out what parameter style is needed for DBAPI compliant drivers here: https://www.python.org/dev/peps/pep-0249/#paramstyle or in the DB driver documentation.

Temporal support
^^^^^^^^^^^^^^^^

Temporal criteria can be added to the tables.

Select
""""""

Here is a select using system time.

.. code-block:: python

    t = Table("abc")
    q = Query.from_(t.for_(SYSTEM_TIME.as_of('2020-01-01'))).select("*")

This produces:

.. code-block:: sql

    SELECT * FROM "abc" FOR SYSTEM_TIME AS OF '2020-01-01'

You can also use between.

.. code-block:: python

    t = Table("abc")
    q = Query.from_(
        t.for_(SYSTEM_TIME.between('2020-01-01', '2020-02-01'))
    ).select("*")

This produces:

.. code-block:: sql

    SELECT * FROM "abc" FOR SYSTEM_TIME BETWEEN '2020-01-01' AND '2020-02-01'

You can also use a period range.

.. code-block:: python

    t = Table("abc")
    q = Query.from_(
        t.for_(SYSTEM_TIME.from_to('2020-01-01', '2020-02-01'))
    ).select("*")

This produces:

.. code-block:: sql

    SELECT * FROM "abc" FOR SYSTEM_TIME FROM '2020-01-01' TO '2020-02-01'

Finally you can select for all times:

.. code-block:: python

    t = Table("abc")
    q = Query.from_(t.for_(SYSTEM_TIME.all_())).select("*")

This produces:

.. code-block:: sql

    SELECT * FROM "abc" FOR SYSTEM_TIME ALL

A user defined period can also be used in the following manner.

.. code-block:: python

    t = Table("abc")
    q = Query.from_(
        t.for_(t.valid_period.between('2020-01-01', '2020-02-01'))
    ).select("*")

This produces:

.. code-block:: sql

    SELECT * FROM "abc" FOR "valid_period" BETWEEN '2020-01-01' AND '2020-02-01'

Joins
"""""

With joins, when the table object is used when specifying columns, it is
important to use the table from which the temporal constraint was generated.
This is because `Table("abc")` is not the same table as `Table("abc").for_(...)`.
The following example demonstrates this.

.. code-block:: python

    t0 = Table("abc").for_(SYSTEM_TIME.as_of('2020-01-01'))
    t1 = Table("efg").for_(SYSTEM_TIME.as_of('2020-01-01'))
    query = (
        Query.from_(t0)
        .join(t1)
        .on(t0.foo == t1.bar)
        .select("*")
    )

This produces:

.. code-block:: sql

    SELECT * FROM "abc" FOR SYSTEM_TIME AS OF '2020-01-01'
    JOIN "efg" FOR SYSTEM_TIME AS OF '2020-01-01'
    ON "abc"."foo"="efg"."bar"

Update & Deletes
""""""""""""""""

An update can be written as follows:

.. code-block:: python

    t = Table("abc")
    q = Query.update(
        t.for_portion(
            SYSTEM_TIME.from_to('2020-01-01', '2020-02-01')
        )
    ).set("foo", "bar")

This produces:

.. code-block:: sql

    UPDATE "abc"
    FOR PORTION OF SYSTEM_TIME FROM '2020-01-01' TO '2020-02-01'
    SET "foo"='bar'

Here is a delete:

.. code-block:: python

    t = Table("abc")
    q = Query.from_(
        t.for_portion(t.valid_period.from_to('2020-01-01', '2020-02-01'))
    ).delete()

This produces:

.. code-block:: sql

    DELETE FROM "abc"
    FOR PORTION OF "valid_period" FROM '2020-01-01' TO '2020-02-01'

Creating Tables
^^^^^^^^^^^^^^^

The entry point for creating tables is ``pypika.Query.create_table``, which is used with the class ``pypika.Column``.
As with selecting data, first the table should be specified. This can be either a
string or a `pypika.Table`. Then the columns, and constraints. Here's an example
that demonstrates much of the functionality.

.. code-block:: python

    stmt = Query \
        .create_table("person") \
        .columns(
            Column("id", "INT", nullable=False),
            Column("first_name", "VARCHAR(100)", nullable=False),
            Column("last_name", "VARCHAR(100)", nullable=False),
            Column("phone_number", "VARCHAR(20)", nullable=True),
            Column("status", "VARCHAR(20)", nullable=False, default=ValueWrapper("NEW")),
            Column("date_of_birth", "DATETIME")) \
        .unique("last_name", "first_name") \
        .primary_key("id")

This produces:

.. code-block:: sql

    CREATE TABLE "person" (
        "id" INT NOT NULL,
        "first_name" VARCHAR(100) NOT NULL,
        "last_name" VARCHAR(100) NOT NULL,
        "phone_number" VARCHAR(20) NULL,
        "status" VARCHAR(20) NOT NULL DEFAULT 'NEW',
        "date_of_birth" DATETIME,
        UNIQUE ("last_name","first_name"),
        PRIMARY KEY ("id")
    )

There is also support for creating a table from a query.

.. code-block:: python

    stmt = Query.create_table("names").as_select(
        Query.from_("person").select("last_name", "first_name")
    )

This produces:

.. code-block:: sql

        CREATE TABLE "names" AS (SELECT "last_name","first_name" FROM "person")

.. _tutorial_end:


.. _license_start:


License
-------

Copyright 2020 KAYAK Germany, GmbH

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

.. |BuildStatus| image:: https://github.com/kayak/pypika/workflows/Unit%20Tests/badge.svg
   :target: https://github.com/kayak/pypika/actions
.. |CoverageStatus| image:: https://coveralls.io/repos/kayak/pypika/badge.svg?branch=master
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
