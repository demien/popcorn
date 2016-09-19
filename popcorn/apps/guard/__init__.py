import time
from machine import Machine
from pool import Pool
from popcorn.apps.base import BaseApp
from popcorn.apps.hub.order.instruction import Operator
from popcorn.utils.log import get_log_obj
from popcorn.rpc.pyro import PyroClient, PyroServer
from .state import ORDERS, add_order


debug, info, warn, error, critical = get_log_obj(__name__)


class Guard(BaseApp):

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        super(Guard, self).init(**kwargs)
        self.rpc_client = PyroClient(self.app.conf['HUB_IP'])
        self.pool = Pool(self.app)
        self.machine = Machine(healthy_mock=app.conf['HEALTHY_MOCK'])
        self.LOOP_INTERVAL = 10  # second

    def start(self):
        """
        Start the guard.
        Setp 1. Start RPC Server
        Step 2. Register to hub
        Step 3. Start loop
        """
        self._start_rpc_server()
        self._register_to_hub()
        self._start_loop()

    def _start_rpc_server(self):
        PyroServer().start()

    def _register_to_hub(self):
        self.rpc_client.call('popcorn.apps.hub:Hub.guard_register', machine=self.machine)

    def _start_loop(self):
        """
        Things to do:
        1. Heartbeat to hub
        2. Check the orders and follow order
        """
        while True:
            try:
                self.heartbeat(self.rpc_client)
                while ORDERS:
                    order = ORDERS.pop(0)
                    debug('[Guard] - [Get Order]: %s' % ','.join([i.cmd for i in order.instructions]))
                    self.follow_order(order)
            except Exception:
                import traceback
                traceback.print_exc()
            finally:
                time.sleep(self.LOOP_INTERVAL)

    def heartbeat(self, rpc_client):
        snapshot = self.machine.snapshot()
        debug('[Guard] - [Send] - [HeartBeat]')
        rpc_client.call('popcorn.apps.hub:Hub.guard_heartbeat', machine=self.machine)

    def follow_order(self, order):
        for instruction in order.instructions:
            pool_name = self.pool.get_or_create_pool_name(instruction.queue)
            if instruction.operator == Operator.INC:
                self.pool.grow(pool_name, instruction.worker_cnt)
            elif instruction.operator == Operator.DEC:
                self.pool.shrink(pool_name, instruction.worker_cnt)

    @staticmethod
    def receive_order(order):
        add_order(order)
