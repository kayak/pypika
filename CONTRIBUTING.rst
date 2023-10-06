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

#. Build the docs locally

    .. code-block:: bash 

        make docs.build

    Open the docs in your browser. For instance, on macOS:

    .. code-block:: bash

        open docs/_build/index.html

#. Run the tests

    .. code-block:: bash 

        make test

    These tests will also run on GitHub Actions when you open a pull request.

Pull Request checklist
----------------------

- Passing tests
- pre-commit hooks passing
- Docstring and examples and checking for correct rendering in the docs

