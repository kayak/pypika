import abc

from pypika.terms import Function


class _AbstractSearchString(Function, metaclass=abc.ABCMeta):
    def __init__(self, name, pattern: str, alias: str = None):
        super(_AbstractSearchString, self).__init__(
            self.clickhouse_function(), name, alias=alias
        )

        self._pattern = pattern

    @classmethod
    @abc.abstractmethod
    def clickhouse_function(cls) -> str:
        pass

    def get_sql(
        self,
        with_alias=False,
        with_namespace=False,
        quote_char=None,
        dialect=None,
        **kwargs
    ):
        args = []
        for p in self.args:
            if hasattr(p, "get_sql"):
                args.append(
                    'toString("{arg}")'.format(
                        arg=p.get_sql(with_alias=False, **kwargs)
                    )
                )
            else:
                args.append(str(p))

        return "{name}({args},'{pattern}'){alias}".format(
            name=self.name,
            args=",".join(args),
            pattern=self._pattern,
            alias=" " + self.alias if self.alias else "",
        )


class Match(_AbstractSearchString):
    @classmethod
    def clickhouse_function(cls) -> str:
        return "match"


class Like(_AbstractSearchString):
    @classmethod
    def clickhouse_function(cls) -> str:
        return "like"


class NotLike(_AbstractSearchString):
    @classmethod
    def clickhouse_function(cls) -> str:
        return "notLike"


class _AbstractMultiSearchString(Function, metaclass=abc.ABCMeta):
    def __init__(self, name, patterns: list, alias: str = None):
        super(_AbstractMultiSearchString, self).__init__(
            self.clickhouse_function(), name, alias=alias
        )

        self._patterns = patterns

    @classmethod
    @abc.abstractmethod
    def clickhouse_function(cls) -> str:
        pass

    def get_sql(
        self,
        with_alias=False,
        with_namespace=False,
        quote_char=None,
        dialect=None,
        **kwargs
    ):
        args = []
        for p in self.args:
            if hasattr(p, "get_sql"):
                args.append(
                    'toString("{arg}")'.format(
                        arg=p.get_sql(with_alias=False, **kwargs)
                    )
                )
            else:
                args.append(str(p))

        return "{name}({args},[{patterns}]){alias}".format(
            name=self.name,
            args=",".join(args),
            patterns=",".join(["'%s'" % i for i in self._patterns]),
            alias=" " + self.alias if self.alias else "",
        )


class MultiSearchAny(_AbstractMultiSearchString):
    @classmethod
    def clickhouse_function(cls) -> str:
        return "multiSearchAny"


class MultiMatchAny(_AbstractMultiSearchString):
    @classmethod
    def clickhouse_function(cls) -> str:
        return "multiMatchAny"
