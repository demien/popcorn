import Pyro4
from popcorn.rpc.base import BaseRPCServer, BaseRPCClient, RPCDispatcher


class PyroServer(BaseRPCServer):

    def __init__(self):
        self.daemon = Pyro4.Daemon()  # make a Pyro daemon
        self._named_server()

    def _named_server(self):
        server = 'popcorn'
        ns = Pyro4.locateNS()                  # find the name server
        uri = self.daemon.register(RPCDispatcher)   # register the greeting maker as a Pyro object
        ns.register(server, uri)   # register the object with a name in the name server

    def _anonymous_server(self):
        uri = self.daemon.register(RPCDispatcher)   # register the greeting maker as a Pyro object

    def start(self):
        self.daemon.requestLoop()                   # start the event loop of the server to wait for calls


class PyroClient(BaseRPCClient):

    def __init__(self, server='popcorn'):
        self.rpc_server = Pyro4.Proxy("PYRONAME:%s" % server)    # use name server object lookup uri shortcut

    def start(self, func_path, *args, **kwargs):
        return self.rpc_server.dispatch(func_path, *args, **kwargs)

