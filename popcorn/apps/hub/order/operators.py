import operator


class BaseOperator(object):

    def __init__(self, slug, apply):
        self.slug = slug
        self.apply = apply

    def __eq__(self, other):
        return self.slug == other.slug


class Operator(object):
    TO = BaseOperator('=', lambda a, b: b)
    INC = BaseOperator('+', operator.add)
    DEC = BaseOperator('-', operator.sub)
    ALL = [TO, INC, DEC]

    @staticmethod
    def get_operator_by_slug(slug):
        for operator in Operator.ALL:
            if operator.slug == slug:
                return operator
