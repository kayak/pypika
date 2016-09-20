# coding: utf8

import ast

from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()


def version():
    path = 'pypika/__init__.py'
    with open(path, 'rU') as file:
        t = compile(file.read(), path, 'exec', ast.PyCF_ONLY_AST)
        for node in (n for n in t.body if isinstance(n, ast.Assign)):
            if len(node.targets) == 1:
                name = node.targets[0]
                if isinstance(name, ast.Name) and \
                        name.id in ('__version__', '__version_info__', 'VERSION'):
                    v = node.value
                    if isinstance(v, ast.Str):
                        return v.s

                    if isinstance(v, ast.Tuple):
                        r = []
                        for e in v.elts:
                            if isinstance(e, ast.Str):
                                r.append(e.s)
                            elif isinstance(e, ast.Num):
                                r.append(str(e.n))
                        return '.'.join(r)


setup(
    # Application name:
    name="PyPika",

    # Version number:
    version=version(),

    # Application author details:
    author="Timothy Heys",
    author_email="theys@kayak.com",

    # License
    license='Apache License Version 2.0',

    # Packages
    packages=["pypika"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://github.com/kayak/pypika",

    description="A SQL query builder API for Python",
    long_description=readme(),

    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: PL/SQL',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
    ],
    keywords=('pypika python query builder querybuilder sql mysql postgres psql oracle vertica aggregated '
              'relational database rdbms business analytics bi data science analysis pandas '
              'orm object mapper'),

    # Dependent packages (distributions)
    install_requires=[
        'aenum'
    ],
    test_suite="pypika.tests",
)
