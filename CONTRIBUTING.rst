Contributing
""""""""""""

In order to contribute, please fork the repository, push your changes to your own repo, then open a pull request on the `PyPika GitHub project`_ project. See `Creating a pull request from a fork <https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork>`_ for more information on how to do this.

Pull Requests
'''''''''''''

1. Fork the repository
2. Clone your forked repository
3. Follow the instructions to setup the project
4. Ensure any new code is covered by adequate test cases.
5. Ensure code is formatted using the Black_ formatter.
6. Write tests as if they were examples on how to use the code.
7. Add documentation for new features.
8. Commit your changes. Write a brief commit message describing what the commit includes.
9. Push your commit(s) to your fork repository.
10. Open a Pull Request on the `PyPika GitHub project`_.


Setup
'''''

PyPika uses the following

- Poetry_ for dependency management and builds
- Black_ for code format
- Sphinx_ for documentation

Installing Dependencies
-----------------------

Create a virtualenv. We recommend using pyenv_ for this. (See also: `installing pyenv <https://github.com/pyenv/pyenv#installation>`_)

``$ pyenv virtualenv {^3.5} pypika-env``

Activate the virtualenv.

``$ pyenv activate pypika-env``

Run a poetry install. (See also: `installing poetry <https://python-poetry.org/docs/#installation>`_)

``$ poetry install``

To update dependencies, update the ``pyproject.toml`` file and then run

``$ poetry update``

.. note:: Make sure to commit changes to the ``poetry.lock`` file.

Installing Pre-commit Hooks
---------------------------

In the project working directory, run

``$ ./install-hook.sh``

This will add a commit hook that runs the Black_ formatter on commit.


Writing Documentation
---------------------

In the root directory of the project, the `README.rst` covers a high level overview of the project, introducing many of the most common features. Everything else should be included in a section in the ``/docs`` directory.


Publishing
----------

To publish a new version:

- Checkout the master branch
- Run the `Poetry Version <https://python-poetry.org/docs/cli/#version>`_ command with the appropriate version bump type.
- Run the `Poetry Publish <https://python-poetry.org/docs/cli/#publish>`_ command

.. note:: Always bump the version from the master branch, never in a feature branch. This is to avoid conflicts.

.. _Pypika GitHub Project: https://github.com/kayak/pypika/pulls
.. _Poetry: https://python-poetry.org/
.. _Black: https://github.com/psf/black
.. _Sphinx: http://www.sphinx-doc.org/
.. _pyenv: https://github.com/pyenv/pyenv

