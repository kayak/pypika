pypika - Python Query Builder
=============================

.. _intro_start:

|BuildStatus|  |CoverageStatus|  |Codacy|  |Docs|  |PyPi|  |License|

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

.. _license_end:

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