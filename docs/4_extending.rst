Extending PyPika
----------------

.. include:: ../README.rst
    :start-after: _appendix_start:
    :end-before:  _appendix_end:


SQL Functions not included in PyPika
""""""""""""""""""""""""""""""""""""

PyPika includes a couple of the most common SQL functions, but due to many differences between different SQL databases,
many are not included. Any SQL function can be implemented in PyPika by extended the ``pypika.terms.Function`` class.

When defining SQL function wrappers, it is necessary to define the name of the SQL function as well as the arguments it
requires.


.. code-block:: python

    from pypika.terms import Function

    class CurDate(Function):
        def __init__(self, alias=None):
            super(CurDate, self).__init__('CURRENT_DATE', alias=alias)

    q = Query.select(CurDate())


.. code-block:: python

    from pypika.terms import Function

    class DateDiff(Function):
        def __init__(self, interval, start_date, end_date, alias=None):
            super(DateDiff, self).__init__('DATEDIFF', interval, start_date, end_date, alias=alias)


There is also a helper function ``pypika.CustomFunction`` which enables 1-line creation of a SQL function wrapper.

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

Similarly analytic functions can be defined by extending ``pypika.terms.AnalyticFunction``.

.. code-block:: python

    from pypika.terms import AnalyticFunction

    class RowNumber(AnalyticFunction):
        def __init__(self, **kwargs):
            super(RowNumber, self).__init__('ROW_NUMBER', **kwargs)


    expr =

    q = Query.from_(self.table_abc) \
            .select(an.RowNumber()
                        .over(self.table_abc.foo)
                        .orderby(self.table_abc.date))

.. code-block:: sql

    SELECT ROW_NUMBER() OVER(PARTITION BY "foo" ORDER BY "date") FROM "abc"

