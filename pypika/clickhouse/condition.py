from pypika.terms import Function


class If(Function):
    def __init__(self, condition, then, else_, alias: str = None, schema: str = None):
        self._else_ = else_
        self._then = then
        self._condition = condition
        self.alias = alias
        self.schema = schema
        self.args = ()
        self.name = 'if'

    def get_sql(self, with_alias=False, with_namespace=False, quote_char=None, dialect=None,
                **kwargs):
        return '{name}({condition},{then},{else_}){alias}'.format(
            name=self.name,
            condition=self._condition,
            then=self._then,
            else_=self._else_,
            alias=' ' + self.alias if self.alias else ''
        )


class MultiIf(Function):
    def __init__(self, conditions: tuple, alias: str = None, schema: str = None):
        """
        :param conditions: condition1, then1, condition2, then2...else
        """
        self._conditions = conditions
        self.alias = alias
        self.schema = schema
        self.args = ()
        self.name = 'multiIf'

    def get_sql(self, with_alias=False, with_namespace=False, quote_char=None, dialect=None,
                **kwargs):
        return '{name}({conditions}){alias}'.format(
            name=self.name,
            conditions=','.join(i.get_sql() for i in self._conditions),
            alias=' ' + self.alias if self.alias else ''
        )
