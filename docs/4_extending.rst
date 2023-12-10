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

Operators not included in PyPika
""""""""""""""""""""""""""""""""""""

PyPika may not include certain operators used in SQL, such as the ``<->``, ``<#>`` or ``<=>`` operators often used in extensions. To incorporate these operators, you can utilize a custom ``ArithmeticExpression`` from ``pypika.terms``.

Custom Operator: ``<->`` for Distance Calculation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For instance, consider a scenario where you want to use the ``<->`` operator in an ``ORDER BY`` clause for geographic distance calculation. You can define this operator using a custom ``ArithmeticExpression``.

.. code-block:: python

    from pypika import CustomFunction, Field, Query
    from pypika.terms import ArithmeticExpression

    # Wrapper class for the custom operator
    class CustomOperator:
        value: str = "<->"  # Define your operator here

    # Custom SQL functions if needed
    ST_SetSRID = CustomFunction("ST_SetSRID", ["geometry", "srid"])
    ST_MakePoint = CustomFunction("ST_MakePoint", ["longitude", "latitude"])

    # Preparing the operands
    left_operand = ST_SetSRID(ST_MakePoint(Field("longitude"), Field("latitude")), 4326)
    right_operand = Field("point")

    # Creating the custom ArithmeticExpression
    orderby_expression = ArithmeticExpression(CustomOperator, left_operand, right_operand)

Using the Custom Operator in a Query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With the custom operator defined, you can now incorporate it into your PyPika queries. For example:

.. code-block:: python

    # Constructing the query with the custom ORDER BY clause
    query = Query.from_("house").select("id").orderby(orderby_expression)

    # The resulting SQL query will be:
    # SELECT "id" FROM "house" ORDER BY ST_SetSRID(ST_MakePoint("longitude", "latitude"), 4326) <-> "point"

Adapting to Other Operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^

This method is not limited to the ``<->`` operator. You can adapt it for any other operator by changing the ``value`` in the ``CustomOperator`` class. This allows for a flexible approach to integrate various operators that are not natively supported in PyPika.


