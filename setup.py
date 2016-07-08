# coding: utf8

import setuptools.command.test
from setuptools import setup


class TestCommand(setuptools.command.test.test):
    def _test_args(self):
        yield 'discover'
        for arg in super(TestCommand, self)._test_args():
            yield arg


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

    #
    # license="LICENSE.txt",
    description="Python Query builder API",

    # Dependent packages (distributions)
    install_requires=[
        'aenum'
    ],
    test_suite="test",
    # cmdclass={'test': TestCommand},
)
