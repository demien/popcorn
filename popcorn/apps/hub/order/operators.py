import operator
from popcorn.apps.exceptions import OperatorApplyException


class _Operator(object):

    def __init__(self, slug, apply):
        self.slug = slug
        self.apply = apply

    def __eq__(self, other):
        return self.slug == other.slug


class Operator(object):
    '''
    Usage: Operator('+').apply(1, 2)
    '''
    TO = _Operator('=', lambda a, b: b)
    INC = _Operator('+', operator.add)
    DEC = _Operator('-', operator.sub)
    ALL = [TO, INC, DEC]

    def __init__(self, slug):
        self._operator = Operator.get(slug)

    def __eq__(self, other):
        return self._operator == other._operator

    def apply(self, *args, **kwargs):
        try:
            return self._operator.apply(*args, **kwargs)
        except:
            raise OperatorApplyException

    @property
    def slug(self):
        return self._operator.slug

    @staticmethod
    def get(slug):
        for operator in Operator.ALL:
            if operator.slug == slug:
                return operator
