from celery import bootsteps
from popcorn.rpc.pyro import PyroServer

class RPCServer(bootsteps.StartStopStep):

    def __init__(self, h, **kwargs):
        h.test = None

    def include_if(self, h):
        return True

    def create(self, h):
        print 'Create RPCServer'
        self.rpc_server = PyroServer()
        return self

    def start(self, h):
        print 'Start RPCServer'
        self.rpc_server.start()

    def stop(self, h):
        print 'in stop'

    def terminate(self, h):
        print 'in terminate'

