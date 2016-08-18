import threading
import time
from celery.utils.imports import instantiate

from popcorn.apps.base import BaseApp
from popcorn.apps.constants import TIME_SCALE
from popcorn.apps.hub import hub_report_demand
from popcorn.apps.hub.order.instruction import WorkerInstruction, InstructionType
from popcorn.apps.hub.state import add_planner
from popcorn.apps.utils import start_back_ground_task, terminate_thread
from popcorn.apps.utils.broker_util import taste_soup
from popcorn.rpc.pyro import RPCClient
from popcorn.utils.log import get_log_obj

debug, info, warn, error, critical = get_log_obj(__name__)
HEARTBEAT_INTERVAL = 5

STRATEGY_MAP = {
    'simple': 'popcorn.apps.planner.strategy.simple:SimpleStrategy'
}


class RegisterPlanner(BaseApp):
    def __init__(self, app, **kwargs):
        self.app = app or self.app
        self.steps = []
        self.setup_defaults(**kwargs)
        self.setup_instance(**kwargs)
        RPCClient(None).create(self)  # this operation will bind rpc_client on self

    def start(self):
        self.rpc_client.start('popcorn.apps.planner:schedule_planner',
                              app=self.app,
                              queue=self.queue,
                              strategy_name=self.strategy_name)

    def setup_defaults(self, loglevel=None, logfile=None, queue=None, strategy=None, **_kw):
        self.queue = self._getopt('queue', queue)
        self.strategy_name = self._getopt('strategy', strategy)
        self.loglevel = self._getopt('log_level', loglevel)
        self.logfile = self._getopt('log_file', logfile)


class Planner(object):
    instances = {

    }  # key queue name , value : instance

    threading_pool = {

    }  # key: planner instance, value : planner's thread

    @classmethod
    def get_instance(cls, app, queue, strategy_name):
        ins = cls.instances.get(queue)
        if not ins:
            ins = Planner(app, queue, strategy_name)
            cls.instances[queue] = ins
            info('[Planner] - [Duplicated] %s', queue)
        return ins

    def __init__(self, app, queue, strategy_name):
        self.app = app or self.app
        self.steps = []
        self.queue = queue
        self.strategy_name = strategy_name
        strategy_cls = STRATEGY_MAP[strategy_name]
        self.strategy = instantiate(strategy_cls)
        self.continue_flag = True
        info('[Planner] - [Register] - Queue: %s, Strategy: %s' % (self.queue, self.strategy_name))
        add_planner(self.queue, self.strategy_name)

    @property
    def alive(self):
        return self.get_thread() is not None

    def restart(self):
        self.start(restart=True)

    def __str__(self):
        return '<Planner %s:%s>' % (self.strategy_name, self.queue)

    def plan(self):

        while True:
            debug('[Planner] - [HeartBeat] - %s , Thread %s', self, threading.current_thread())
            previous_timestamp = int(round(time.time() * TIME_SCALE))
            previous_status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
            time.sleep(HEARTBEAT_INTERVAL)
            timestamp = int(round(time.time() * TIME_SCALE))
            status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
            result = self.strategy.apply(
                previous_status=previous_status,
                previous_time=previous_timestamp,
                status=status,
                time=timestamp
            )
            if self.continue_flag != True:
                info('[Planner] - [Stop Scan] ...')
                break
            if result:
                cmd = WorkerInstruction.generate_instruction_cmd(self.queue, result)
                info('[Planner] - [Report New Demand] - %s' % str(cmd))
                hub_report_demand(type=InstructionType.WORKER, cmd=cmd)

    def start(self, restart=True):
        debug('[Planner] - [Start(restart=%s)] - %s' % (restart, self))
        if self.get_thread():
            if restart:
                self.stop()
                self.start(restart=False)
            else:
                raise Exception('thread already exist')
        else:
            self.continue_flag = True
            Planner.threading_pool[self] = start_back_ground_task(self.plan)

    def stop(self, blocking=True):
        debug('[Planner] - [Stop] - %s' % self)
        thread = self.pop_thread()
        if thread is None:
            return
        else:
            self.continue_flag = False
            if blocking:
                thread.join(timeout=10)
            else:
                start_back_ground_task(self.force_quit, args=(thread,))

    def force_quit(self, thread):
        time.sleep(HEARTBEAT_INTERVAL * 2)
        terminate_thread(thread)

    def get_thread(self):
        return Planner.threading_pool.get(self, None)

    def pop_thread(self):
        return Planner.threading_pool.pop(self, None)


def schedule_planner(app, queue, strategy_name):
    debug('Schedule planner : queue %s:%s', queue, strategy_name)
    planner = Planner.get_instance(app, queue, strategy_name)
    if planner.alive:
        planner.restart()
    else:
        planner.start()
