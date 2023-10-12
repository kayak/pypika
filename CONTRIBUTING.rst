Guidelines for Contributing
===========================

PyPika welcomes contributions in all forms. These may be bugs, feature requests, documentation, or examples. Please feel free to:

#. Submitting an issue
#. Opening a pull request
#. Helping with outstanding issues and pull requests 

Open an issue
-------------

If you find a bug or have a feature request, please `open an issue <https://github.com/kayak/pypika/issues>`_ on GitHub. Please just check that the issue doesn't already exist before opening a new one.

Local development steps
-----------------------

Create a forked branch of the repo
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Do this once but keep it up to date

#. `Fork the kayak/PyPika repo GitHub <https://github.com/kayak/pypika/fork>`_
#. Clone forked repo and set upstream

    .. code-block:: bash

        git clone git@github.com:<your-username>/pypika.git
        cd pypika
        git remote add upstream git@github.com:kayak/pypika.git

Setup local development environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Setup up python virtual environment

    Create and activate the environment. Here is an example using ``venv`` from the standard library:

    .. code-block:: bash

        python -m venv .venv
        source .venv/bin/activate

#. Install python dependencies for development

    With the virtual environment activated, install the development requirements, pre-commit, and the package itself:

    .. code-block:: bash

        make install

#. Run the tests

    The unit tests are run with ``unittest`` via ``tox``. To run the tests locally:

    .. code-block:: bash 

        make test

    These tests will also run on GitHub Actions when you open a pull request.

#. Build the docs locally

    Our docs are built with Sphinx. To build the docs locally:

    .. code-block:: bash 

        make docs.build

    Open the docs in your browser. For instance, on macOS:

    .. code-block:: bash

        open docs/_build/index.html

Pull Request checklist
----------------------

Please check that your pull request meets the following criteria:

- Unit tests pass
- pre-commit hooks pass
- Docstring and examples and checking for correct render in the docs

Documentation
-------------

Documentation is built with Sphinx and hosted on `Read the Docs <https://pypika.readthedocs.io/en/latest/>`_. The latest builds are displayed on their site `here <https://readthedocs.org/projects/pypika/builds/>`_.

The code documentation is to be written in the docstrings of the code itself or in the various ``.rst`` files in project root or the ``docs/`` directory.

The docstrings can be in either `Numpy <https://numpydoc.readthedocs.io/en/latest/format.html>`_ or `Sphinx <https://sphinx-rtd-tutorial.readthedocs.io/en/latest/docstrings.html>`_ format.

Automations
-----------

We use `pre-commit <https://pre-commit.com/>`_ to automate format checks. Install the pre-commit hooks with the ``make install`` command described above.

GitHub Actions runs both format checks and unit tests on every pull request.

**NOTE:** The hosted documentation is built separately from the GitHub Actions workflow. To build the docs locally, use the ``make docs.build`` command described above.