import abc


class BaseStrategy(object):

    __metaclass__ = abc.ABCMeta

    def apply(self, **kwargs):
        raise NotImplementedError()
