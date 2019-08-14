from pypika.terms import Function


class ToString(Function):
    def __init__(self, value, alias: str = None):
        super(ToString, self).__init__('toString', value, alias=alias)


class ToFixedString(Function):
    def __init__(self, value, length: int, alias: str = None):
        super(ToFixedString, self).__init__('toFixedString', value, alias=alias)
        self._length = length

    def get_sql(self, with_alias=False, with_namespace=False, quote_char=None, dialect=None,
                **kwargs):
        return '{name}("{args}",{length}){alias}'.format(
            name=self.name,
            args=','.join(p.get_sql(with_alias=False, **kwargs)
                          if hasattr(p, 'get_sql')
                          else str(p)
                          for p in self.args),
            length=self._length,
            alias=' ' + self.alias if self.alias else ''
        )


class ToInt8(Function):
    def __init__(self, value, alias: str = None):
        super(ToInt8, self).__init__('toInt8', value, alias=alias)


class ToInt16(Function):
    def __init__(self, value, alias: str = None):
        super(ToInt16, self).__init__('toInt16', value, alias=alias)


class ToInt32(Function):
    def __init__(self, value, alias: str = None):
        super(ToInt32, self).__init__('toInt32', value, alias=alias)


class ToInt64(Function):
    def __init__(self, value, alias: str = None):
        super(ToInt64, self).__init__('toInt64', value, alias=alias)


class ToUInt8(Function):
    def __init__(self, value, alias: str = None):
        super(ToUInt8, self).__init__('toUInt8', value, alias=alias)


class ToUInt16(Function):
    def __init__(self, value, alias: str = None):
        super(ToUInt16, self).__init__('toUInt16', value, alias=alias)


class ToUInt32(Function):
    def __init__(self, value, alias: str = None):
        super(ToUInt32, self).__init__('toUInt32', value, alias=alias)


class ToUInt64(Function):
    def __init__(self, value, alias: str = None):
        super(ToUInt64, self).__init__('toUInt64', value, alias=alias)


class ToFloat32(Function):
    def __init__(self, value, alias: str = None):
        super(ToFloat32, self).__init__('toFloat32', value, alias=alias)


class ToFloat64(Function):
    def __init__(self, value, alias: str = None):
        super(ToFloat64, self).__init__('toFloat64', value, alias=alias)


class ToDate(Function):
    def __init__(self, value, alias: str = None):
        super(ToDate, self).__init__('toDate', value, alias=alias)


class ToDateTime(Function):
    def __init__(self, value, alias: str = None):
        super(ToDateTime, self).__init__('toDateTime', value, alias=alias)
