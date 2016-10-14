import time
import threading
from machine import Machine
from pool import Pool
from popcorn.apps.base import BaseApp
from popcorn.apps.exceptions import CouldNotStopException, CouldNotStartException
from popcorn.apps.hub.order.instruction import Operator
from popcorn.utils import get_log_obj, get_pid, wait_condition_till_timeout
from popcorn.rpc.pyro import PyroClient, PyroServer, GUARD_PORT, HUB_PORT
from .state import ORDERS, add_order


debug, info, warn, error, critical = get_log_obj(__name__)


class Guard(BaseApp):

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        super(Guard, self).init(**kwargs)
        self.rpc_server = PyroServer(GUARD_PORT)
        self.rpc_client = PyroClient(self.app.conf['HUB_IP'], HUB_PORT)
        self.pool = Pool(self.app)
        self.machine = Machine(healthy_mock=app.conf['HEALTHY_MOCK'])
        self.__shutdown_guard = threading.Event()
        self.LOOP_INTERVAL = 10  # second
        self.alive = False

    def start(self, condition=lambda: True):
        """
        Start the guard.
        Setp 1. Start RPC Server
        Step 2. Register to hub
        Step 3. Start loop
        """
        self.__shutdown_guard.clear()
        self._start_rpc_server()
        self._register_to_hub()
        self._start_loop(condition)

    def stop(self):
        '''
        Step 1. Unregister to hub (todo)
        Step 2. Stop rpc server
        Step 3. Stop the loop
        '''
        if self.rpc_server.alive:
            self.rpc_server.stop()
        self.__shutdown_guard.set()
        if wait_condition_till_timeout(self.is_alive, 10):
            raise CouldNotStopException('guard')

    def is_alive(self):
        return self.alive

    def _start_rpc_server(self):
        self.rpc_server.start()

    def _register_to_hub(self):
        self.rpc_client.call('popcorn.apps.hub.commands.register_machine', machine=self.machine)

    def _start_loop(self, condition):
        """
        Things to do:
        1. Heartbeat to hub
        2. Check the orders and follow order
        """
        self.alive = True
        while not self.__shutdown_guard.isSet() and condition():
            try:
                self.heartbeat(self.rpc_client)
                while ORDERS:
                    order = ORDERS.pop(0)
                    debug('[Guard] - [Get Order]: %s' % ','.join([i.cmd for i in order.instructions]))
                    self.follow_order(order)
            except Exception as e:
                error('[Guard] - [Exception] - [Loop] : %s. PID: %s', e.message, get_pid())
            finally:
                time.sleep(self.LOOP_INTERVAL)
        self.alive = False

    def heartbeat(self, rpc_client):
        self.machine.snapshot(self.pool.pinfo)
        debug('[Guard] - [Send] - [HeartBeat]')
        rpc_client.call('popcorn.apps.hub.commands.update_machine', machine=self.machine)

    def follow_order(self, order):
        for instruction in order.instructions:
            if instruction.operator == Operator.INC:
                self.pool.grow(instruction.queue, instruction.worker_cnt)
            elif instruction.operator == Operator.DEC:
                self.pool.shrink(instruction.queue, instruction.worker_cnt)
