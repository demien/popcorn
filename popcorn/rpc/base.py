import abc
from celery.utils.imports import instantiate


class BaseRPCServer(object):

    __metaclass__ = abc.ABCMeta


    def start(self):
        raise NotImplementedError()


class BaseRPCClient(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, server='popcorn'):
        self.rpc_server = Pyro4.Proxy("PYRONAME:%s" % server)    # use name server object lookup uri shortcut

    def start(self, func_path):
        raise NotImplementedError()
        self.rpc_server.dispatch(func_path)


class RPCDispatcher(object):

    def dispatch(self, func_path, *args, **kwargs):
        print("Dispatch %s" % func_path)
        r = instantiate(func_path, **kwargs)
        print r
