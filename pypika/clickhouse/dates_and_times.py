from pypika.terms import Function


class FormatDateTime(Function):
    def __init__(self, term, alt, **kwargs):
        super().__init__('formatDateTime', term, alt, **kwargs)
