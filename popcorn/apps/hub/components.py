import Pyro4
import socket
import time

from celery import bootsteps
from multiprocessing import Process
from popcorn.rpc.pyro import PyroServer


class NameSpaceServer(bootsteps.StartStopStep):

    def __init__(self, h, **kwargs):
        self.port = 9090

    def include_if(self, h):
        return True

    def create(self, h):
        return self

    def start(self, h):
        host = self.get_host()
        port = self.port
        p = Process(target=self.start_name_space_server, args=(host, port))
        p.start()
        time.sleep(3)  # fixme: too simple too naive
        print '[RPC Name Space Server] Start'

    def stop(self, h):
        print 'in stop'

    def terminate(self, h):
        print 'in terminate'

    def start_name_space_server(self, host, port):
        Pyro4.naming.startNSloop()

    def get_host(self):
        return socket.gethostname()
        return '127.0.0.1'


class RPCServer(bootsteps.StartStopStep):

    def __init__(self, h, **kwargs):
        h.test = None

    def include_if(self, h):
        return True

    def create(self, h):
        self.rpc_server = PyroServer()
        return self

    def start(self, h):
        print '[RPC Server] Start'
        self.rpc_server.start()

    def stop(self, h):
        print 'in stop'

    def terminate(self, h):
        print 'in terminate'


class RPCClient(bootsteps.StartStopStep):

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, h):
        return True

    def create(self, h):
        self.rpc_client = PyroClient()()
        return self

    def start(self, h):
        print '[RPC Server] Start'
        self.rpc_server.start()

    def stop(self, h):
        print 'in stop'

    def terminate(self, h):
        print 'in terminate'
