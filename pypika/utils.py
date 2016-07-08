# coding: utf8

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"
__version__ = "0.0.1"


class QueryException(Exception):
    pass


class GroupingException(Exception):
    pass


class CaseException(Exception):
    pass


class JoinException(Exception):
    pass


class UnionException(Exception):
    pass


def immutable(func):
    import copy

    def _decorator(self, *args, **kwargs):
        self_copy = copy.deepcopy(self)
        return func(self_copy, *args, **kwargs)

    return _decorator
