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


debug, info, warn, error, critical = get_log_obj(__name__)


class Guard(BaseApp):

    class Blueprint(bootsteps.Blueprint):
        """Hub bootstep blueprint."""
        name = 'Guard'
        default_steps = set([
            'popcorn.rpc.pyro:RPCClient',
            'popcorn.apps.guard:Register',
            'popcorn.apps.guard:Loop',
        ])

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        super(Guard, self).init(**kwargs)
        self.steps = []
        self.processes = defaultdict(list)
        self.pool = Pool(self.app)
        self.machine = Machine(healthy_mock=app.conf['HEALTHY_MOCK'])
        self.blueprint = self.Blueprint(app=self.app)
        self.blueprint.apply(self)

    @property
    def id(self):
        return self.machine.id

    def start(self):
        self.blueprint.start(self)

    def loop(self, rpc_client):
        while True:
            try:
                order = self.heartbeat(rpc_client)
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


class Register(bootsteps.StartStopStep):
    requires = (RPCClient,)

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, p):
        return True

    def create(self, p):
        p.rpc_client.call('popcorn.apps.hub:Hub.guard_register', machine=p.machine)
        return self

    def start(self, p):
        pass

    def stop(self, p):
        print 'in stop'

    def terminate(self, p):
        print 'in terminate'


class Loop(bootsteps.StartStopStep):
    requires = (Register,)

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, p):
        return True

    def create(self, p):
        return self

    def start(self, p):
        p.loop(p.rpc_client)
        pass

    def stop(self, p):
        print 'in stop'

    def terminate(self, p):
        print 'in terminate'
