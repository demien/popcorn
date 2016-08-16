from popcorn.commands import BaseCommand
import time
import logging
from celery import bootsteps
from celery.utils.imports import instantiate
from multiprocessing import Process
from popcorn.apps.base import BaseApp
from popcorn.apps.constants import TIME_SCALE
from popcorn.apps.hub import Hub
from popcorn.apps.utils.broker_util import taste_soup
from popcorn.rpc.pyro import RPCClient
from popcorn.apps.hub.order.instruction import WorkerInstruction, InstructionType
from popcorn.apps.hub.state import add_planner
from popcorn.utils.log import get_log_obj


debug, info, warn, error, critical = get_log_obj(__name__)
HEARTBEAT_INTERVAL = 5


STRATEGY_MAP = {
    'simple': 'popcorn.apps.planner.strategy.simple:SimpleStrategy'
}


class RegisterPlanner(BaseApp):

    class Blueprint(bootsteps.Blueprint):
        """Hub bootstep blueprint."""
        name = 'RegisterPlanner'
        default_steps = set([
            'popcorn.rpc.pyro:RPCClient',
            'popcorn.apps.planner:Register',
        ])

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        self.steps = []
        self.setup_defaults(**kwargs)
        self.setup_instance(**kwargs)
        self.blueprint = self.Blueprint(app=self.app)
        self.blueprint.apply(self)

    def start(self):
        self.blueprint.start(self)

    def setup_defaults(self, loglevel=None, logfile=None, queue=None, strategy=None, **_kw):
        self.queue = self._getopt('queue', queue)
        self.strategy_name = self._getopt('strategy', strategy)
        self.loglevel = self._getopt('log_level', loglevel)
        self.logfile = self._getopt('log_file', logfile)


class Planner(object):

    class Blueprint(bootsteps.Blueprint):

        """Hub bootstep blueprint."""
        name = 'Planner'
        default_steps = set([
            'popcorn.rpc.pyro:RPCClient',
            'popcorn.apps.planner:Strategy',
            'popcorn.apps.planner:Loop',
        ])

    def __init__(self, app, queue, strategy_name):
        self.app = app or self.app
        self.steps = []
        self.blueprint = self.Blueprint(app=self.app)
        self.queue = queue
        self.strategy_name = strategy_name
        self.blueprint.apply(self)

    def start(self):
        info('[Planner] - [Register] - Queue: %s, Strategy: %s' % (self.queue, self.strategy_name))
        add_planner(self.queue, self.strategy_name)
        self.blueprint.start(self)

    def plan(self):
        while True:
            debug('[Planner] - [HeartBeat] - %s:%s' % (self.queue, self.strategy_name))
            previous_timestampe = int(round(time.time() * TIME_SCALE))
            previous_status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
            time.sleep(HEARTBEAT_INTERVAL)
            timestampe = int(round(time.time() * TIME_SCALE))
            status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
            result = self.strategy.apply(
                previous_status=previous_status,
                previous_time=previous_timestampe,
                status=status,
                time=timestampe
            )
            if result:
                cmd = WorkerInstruction.generate_instruction_cmd(self.queue, result)
                info('[Planner] - [Report New Demand] - %s' % str(cmd))
                self.rpc_client.start('popcorn.apps.hub:hub_report_demand', type=InstructionType.WORKER, cmd=cmd)


class Strategy(bootsteps.StartStopStep):

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, p):
        return True

    def create(self, p):
        strategy_cls = STRATEGY_MAP[p.strategy_name]
        p.strategy = instantiate(strategy_cls)
        return self

    def start(self, p):
        pass

    def stop(self, p):
        print 'in stop'

    def terminate(self, p):
        print 'in terminate'


class Loop(bootsteps.StartStopStep):
    requires = (Strategy, RPCClient, )

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, p):
        return True

    def create(self, p):
        self.process = Process(target=p.plan)
        self.process.start()
        return self

    def start(self, p):
        pass

    def stop(self, p):
        print 'in stop'

    def terminate(self, p):
        print 'in terminate'


class Register(bootsteps.StartStopStep):
    requires = (RPCClient, )

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, p):
        return True

    def create(self, p):
        return self

    def start(self, p):
        p.rpc_client.start('popcorn.apps.planner:Planner', app=p.app, queue=p.queue, strategy_name=p.strategy_name)

    def stop(self, p):
        print 'in stop'

    def terminate(self, p):
        print 'in terminate'
