# Guidelines for Contributing

PyPika welcomes contributions in all forms. These include: 

1. Submitting an issue to report a bug or request a feature
2. 


## Open an issue

If you find a bug or have a feature request, please [open an issue](https://github.com/kayak/pypika/issues) on GitHub.

## Local development steps

### Create a forked branch of the repo

Do this onces but keep it up to date

1. [Fork the kayak/PyPika repo GitHub](https://github.com/kayak/pypika/fork)
2. Clone forked repo and set upstream

```bash
git clone git@github.com:<your-username>/pypika.git
cd pypika
git remote add upstream git@github.com:kayak/pypika.git
```

### Setup local development environment

1. Setup up python virtual environment

Create and activate the environment. Here is an example using `venv` from the standard library:

```bash 
python -m venv .venv
source .venv/bin/activate
```

2. Install pre-commit 

pre-commit is use for checking code style

```bash
python -m pip install pre-commit
pre-commit install 
```

2. Install python dependecies for development

Install dev requirements

... code-block:: bash

    python -m pip install -r requirements-dev.txt

Install an editable install of 

```bash
python -m pip install -e .
```

3. Build the docs locally

```bash 
sphinx-build -b html docs docs/_build
```

Open the docs in your browser. For instance, on macOS:

```bash
open docs/_build/index.html
```

4. Run the tests

The tests can be run locally using `tox`:

```bash 
tox 
```

These tests will also be run on GitHub Actions when you open a pull request.

## Pull Request checklist

- Add passing tests 
- Add docstring when possible!