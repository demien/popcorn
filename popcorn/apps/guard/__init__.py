import psutil
import socket
import time
from celery import bootsteps
from collections import defaultdict
from machine import Machine
from pool import Pool
from popcorn.apps.base import BaseApp
from popcorn.apps.hub.order.instruction import Operator
from popcorn.rpc.pyro import RPCClient
from popcorn.utils.log import get_log_obj
from popcorn.rpc.pyro import PyroClient


debug, info, warn, error, critical = get_log_obj(__name__)


class Guard(BaseApp):

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        super(Guard, self).init(**kwargs)

        self.rpc_client = PyroClient(self.app.conf['HUB_IP'])
        self.pool = Pool(self.app)
        self.machine = Machine(healthy_mock=app.conf['HEALTHY_MOCK'])

    @property
    def id(self):
        return self.machine.id

    def start(self):
        """
        Start the guard.
        Step 1. Register to rpc server
        Step 2. Start loop
        """
        self._register_to_rpc_server()
        self._start_loop()

    def _register_to_rpc_server(self):
        self.rpc_client.call('popcorn.apps.hub:Hub.guard_register', machine=self.machine)

    def _start_loop(self):
        while True:
            try:
                order = self.heartbeat(self.rpc_client)
                if order:
                    debug('[Guard] - [Get Order]: %s' % ','.join([i.cmd for i in order.instructions]))
                    self.follow_order(order)
            except Exception:
                import traceback
                traceback.print_exc()
            finally:
                time.sleep(5)

    def heartbeat(self, rpc_client):
        snapshot = self.machine.snapshot()
        debug('[Guard] - [Send] - [HeartBeat]')
        return rpc_client.call('popcorn.apps.hub:Hub.guard_heartbeat', machine=self.machine)

    def follow_order(self, order):
        for instruction in order.instructions:
            pool_name = self.pool.get_or_create_pool_name(instruction.queue)
            
            if instruction.operator == Operator.INC:
                self.pool.grow(pool_name, instruction.worker_cnt)
            elif instruction.operator == Operator.DEC:
                self.pool.shrink(pool_name, instruction.worker_cnt)
