from pypika import Field
from pypika.terms import Function


class IfNull(Function):
    def __init__(self, field, alt, alias: str = None, schema: str = None):
        self._field = field
        self._alt = alt
        self.alias = alias
        self.name = 'ifNull'
        self.args = ()
        self.schema = schema

    def _field_to_str(self, field):
        if field == 'NULL':
            return field
        if isinstance(self._field, Field):
            return self._field.get_sql()
        return field

    def get_sql(self, with_alias=False, with_namespace=False, quote_char=None, dialect=None,
                **kwargs):
        return '{name}({field},{alt}){alias}'.format(
            name=self.name,
            field=self._field_to_str(self._field),
            alt=self._field_to_str(self._alt),
            alias=' ' + self.alias if self.alias else ''
        )
