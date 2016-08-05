# coding: utf8

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


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


class RollupException(Exception):
    pass


def builder(func):
    import copy

    def _decorator(self, *args, **kwargs):
        self_copy = copy.deepcopy(self)
        return func(self_copy, *args, **kwargs) or self_copy

    return _decorator
