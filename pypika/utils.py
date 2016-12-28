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


def resolve_is_aggregate(values):
    """
    Resolves the is_aggregate flag for an expression that contains multiple terms.  This works like a voter system,
    each term votes True or False or abstains with None.

    :param values: A list of booleans (or None) for each term in the expression
    :return: If all values are True or None, True is returned.  If all values are None, None is returned. Otherwise,
        False is returned.
    """
    result = [x
              for x in values
              if x is not None]
    if result:
        return all(result)
    return None
