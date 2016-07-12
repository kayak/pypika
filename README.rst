pypika - Python Query Builder
=============================

.. _intro_start:

|BuildStatus|  |CoverageStatus|  |Codacy|  |Docs|  |PyPi|

*PyPika* is a Python API for building SQL queries. The motivation behind *PyPika* is to provide a simple interface for
building SQL queries without limiting the flexibility of handwritten SQL. Designed with data analysis in mind, *PyPika*
leverages the builder pattern design to construct queries to avoid messy string formatting and concatenation. It is also
easily extended to take full advantage of specific features of SQL database vendors.

.. _intro_end:

.. _installation_start:

Installation
------------

*PyPika* supports python ``2.7`` and ``3.3+``.  It may also work on pypy, cython, and jython, but is not being tested for these versions.

To install *PyPika* run the following command:

.. code-block:: bash

    pip install pypika


.. _installation_end:

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


.. _available_badges_end: