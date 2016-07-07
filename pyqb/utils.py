# coding: utf8
import copy

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


def immutable(func):
    def _decorator(self, *args, **kwargs):
        self_copy = copy.deepcopy(self)
        return func(self_copy, *args, **kwargs)

    return _decorator

