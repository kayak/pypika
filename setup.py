# coding: utf8

from setuptools import setup

__major_version__ = 0
__minor_version__ = 0
__patch_version__ = 12


def readme():
    with open('README.rst') as f:
        return f.read()


setup(
    # Application name:
    name="PyPika",

    # Version number (initial):
    version='{major}.{minor}.{patch}'.format(major=__major_version__,
                                             minor=__minor_version__,
                                             patch=__patch_version__),

    # Application author details:
    author="Timothy Heys",
    author_email="theys@kayak.com",

    # Packages
    packages=["pypika"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://github.com/kayak/pypika",

    # License
    # license="LICENSE.txt",
    description="A query builder API for Python",
    long_description=readme(),

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
    keywords=('pypika python query builder querybuild sql mysql postgres psql oracle vertica aggregated '
              'relational database rdbms business analytics bi data science analysis pandas '
              'orm object mapper'),

    # Dependent packages (distributions)
    install_requires=[
        'aenum'
    ],
    test_suite="pypika.tests",
)
