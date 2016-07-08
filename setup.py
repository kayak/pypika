# coding: utf8

from setuptools import setup

setup(
    # Application name:
    name="PyQB",

    # Version number (initial):
    version="0.0.1",

    # Application author details:
    author="Timothy Heys",
    author_email="theys@kayak.com",

    # Packages
    packages=["pyqb"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://github.com/kayak/pyqb",

    # License
    # license="LICENSE.txt",
    description="A query builder API for Python",

    classifiers=[
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
    keywords=('pyqb python query builder querybuild sql mysql postgres psql oracle vertica aggregated '
              'relational database rdbms business analytics bi data science analysis pandas '
              'orm object mapper'),

    # Dependent packages (distributions)
    install_requires=[
        'aenum'
    ],
    test_suite="test",
)
