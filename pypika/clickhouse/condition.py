from pypika import utils
from pypika.terms import Function


class If(Function):
    def __init__(self, condition, then, else_, alias: str = None, schema: str = None):
        self._else_ = else_
        self._then = then
        self._condition = condition
        self.alias = alias
        self.schema = schema
        self.name = 'if'

    def get_sql(self, with_alias=False, with_namespace=False, quote_char=None, dialect=None,
                **kwargs):
        return utils.alias_sql(
            '{name}({condition},{then},{else_})'.format(
                name=self.name,
                condition=self._condition,
                then=self._then,
                else_=self._else_,
            ),
            self.alias
        )


class MultiIf(Function):
    def __init__(self, *conditions, **kwargs):
        super().__init__('multiIf', *conditions, **kwargs)
