import abc
import Pyro4
from popcorn.utils import call_callable


class BaseRPCServer(object):
    __metaclass__ = abc.ABCMeta

    def start(self):
        raise NotImplementedError()


class BaseRPCClient(object):
    __metaclass__ = abc.ABCMeta

    def call(self, func_path):
        raise NotImplementedError()

@Pyro4.expose
class RPCDispatcher(object):

    def dispatch(self, func_path, *args, **kwargs):
        return call_callable(func_path, *args, **kwargs)
