import abc
from popcorn.utils import instantiate


class BaseRPCServer(object):
    __metaclass__ = abc.ABCMeta

    def start(self):
        raise NotImplementedError()


class BaseRPCClient(object):
    __metaclass__ = abc.ABCMeta

    def call(self, func_path):
        raise NotImplementedError()


class RPCDispatcher(object):

    def dispatch(self, func_path, *args, **kwargs):
        instantiate(func_path, **kwargs)

    def dispatch_with_return(self, func_path, *args, **kwargs):
        return instantiate(func_path, **kwargs)
