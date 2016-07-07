from distutils.core import setup

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
)
