import copy
import Pyro4
import socket
import threading
from celery import bootsteps
from popcorn.rpc import DISPATHCER_SERVER_OBJ_ID, GUARD_PORT, HUB_PORT
from popcorn.rpc.base import BaseRPCServer, BaseRPCClient, RPCDispatcher
from popcorn.utils import get_log_obj, get_pid


debug, info, warn, error, critical = get_log_obj(__name__)
__all__ = ['PyroServer', 'PyroClient']

DEFAULT_SERIALIZER = 'pickle'
DEFAULT_SERVERTYPE = 'multiplex'
# DEFAULT_SERVERTYPE = 'thread'


class PyroBase(object):

    def __init__(self, **kwargs):
        Pyro4.config.SERVERTYPE = DEFAULT_SERVERTYPE
        Pyro4.config.SERIALIZER = DEFAULT_SERIALIZER


class PyroServer(BaseRPCServer, PyroBase):

    def __init__(self, port):
        PyroBase.__init__(self)
        self.port = port
        self.daemon = None
        self.thread = None

    @property
    def ip(self):
        host = socket.gethostname()
        return socket.gethostbyname(host)

    @property
    def alive(self):
        if self.thread is not None and self.thread.is_alive():
            return True
        return False

    def start_daemon(self):
        if self.daemon is None or self.daemon.transportServer is None:
                self.daemon = Pyro4.Daemon(host=self.ip, port=self.port)  # init a Pyro daemon

    def start(self):
        """
        Start a pyro server

        Fire a new thread for the server daemon loop.
        This mehtod is blocking till the server daemon loop is ready.
        """
        self.start_daemon()
        uri = self.register(RPCDispatcher, DISPATHCER_SERVER_OBJ_ID)
        thread = threading.Thread(target=self.daemon.requestLoop)
        thread.daemon = True
        thread.start()
        while not thread.is_alive():
            continue
        self.thread = thread
        info('[RPC Server] - [Start] - %s.' % uri)

    def stop(self):
        """
        Stop the pyro server

        Notice. the step order is quite important and can not change.
        Step 1: stop the daemon loop
        Step 2: stop the socket server
        Step 3: unregister the dispather class
        """
        self.daemon.shutdown()
        if self.thread is not None and self.thread.is_alive():
            while self.thread.is_alive():
                continue
        self.daemon.close()
        self.unregister(RPCDispatcher)
        info('[RPC Server] - [Shutdown] - exit daemon loop')

    def register(self, obj, obj_id=None):
        """
        Register the obj to the server.
        """
        try:
            return self.daemon.register(obj, obj_id)  # register a obj with obj id
        except:
            return self.daemon.uriFor(obj_id)

    def unregister(self, obj):
        """
        Unregister the obj from the server.
        Ignore if unregister an unexist obj.
        """
        try:
            return self.daemon.unregister(obj)
        except Exception as e:
            pass  # don't care for multi unregister


class PyroClient(BaseRPCClient, PyroBase):

    def __init__(self, server_ip, server_port):
        PyroBase.__init__(self)
        dispatcher_uri = self.get_uri(DISPATHCER_SERVER_OBJ_ID, server_ip, server_port)
        self.default_proxy = self.get_proxy_obj(dispatcher_uri)  # get local proxy obj

    def call(self, path, *args, **kwargs):
        """
        Call a remote obj or class.

        :param str path: the path of the callable obj. A valid one: popcorn.apps.hub:Hub.scan.
         More detail of path please check popcorn.utils.imports.symbol_by_name
        """
        return self.default_proxy.dispatch(path, *args, **kwargs)

    def get_proxy_obj(self, uri):
        return Pyro4.Proxy(uri)

    def get_uri(self, obj_id, server_ip, port):
        return 'PYRO:%s@%s:%s' % (str(obj_id), str(server_ip), str(port))
