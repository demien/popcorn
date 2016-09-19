import time

from popcorn.apps.base import BaseApp
from popcorn.apps.constants import TIME_SCALE
from popcorn.apps.hub import Hub
from popcorn.apps.hub.order.instruction import InstructionType
from popcorn.apps.utils.broker_util import taste_soup
from popcorn.rpc.pyro import PyroClient
from popcorn.utils import get_log_obj, get_pid, start_daemon_thread, terminate_thread, call_callable

debug, info, warn, error, critical = get_log_obj(__name__)
HEARTBEAT_INTERVAL = 5


STRATEGY_MAP = {
    'simple': 'popcorn.apps.planner.strategy.simple:SimpleStrategy'
}


class RegisterPlanner(BaseApp):
    def __init__(self, app, **kwargs):
        self.app = app or self.app
        super(RegisterPlanner, self).init(**kwargs)
        self.rpc_client = PyroClient(self.app.conf['HUB_IP'])

    def start(self):
        self.rpc_client.call('popcorn.apps.planner:schedule_planner',
                              app=self.app,
                              queue=self.queue,
                              strategy_name=self.strategy_name)

    def setup_defaults(self, loglevel=None, logfile=None, queue=None, strategy=None, **_kw):
        super(RegisterPlanner, self).setup_defaults(loglevel=None, logfile=None, queue=None, strategy=None, **_kw)
        self.queue = self._getopt('queue', queue)
        self.strategy_name = self._getopt('strategy', strategy)


class Planner(object):
    instances = {}  # key queue name , value : instance

    @classmethod
    def get_instance(cls, app, queue, strategy_name):
        ins = cls.instances.get(queue)
        if not ins:
            ins = Planner(app, queue, strategy_name)
            cls.instances[queue] = ins
        return ins

    @classmethod
    def stats(cls):
        return {q: str(v) for q, v in cls.instances.items()}

    def __init__(self, app, queue, strategy_name):
        self.app = app or self.app
        self.queue = queue
        self.strategy_name = strategy_name
        self.strategy = call_callable(STRATEGY_MAP[strategy_name])
        self.continue_flag = True
        info('[Planner] - [Register] - Queue: %s, Strategy: %s' % (self.queue, self.strategy_name))
        self.thread = None

    @property
    def alive(self):
        return self.thread is not None

    def restart(self):
        self.start(restart=True)

    def __repr__(self):
        return 'Queue: %s Strategy: %s' % (self.strategy_name, self.queue)

    def plan(self):
        status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
        while True:
            debug('[Planner] - [Start] - [HeartBeat] : %s. PID: %s', self, get_pid())
            previous_timestamp = int(round(time.time() * TIME_SCALE))
            previous_status = status
            time.sleep(HEARTBEAT_INTERVAL)
            status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
            result = self.strategy.apply(
                previous_status=previous_status,
                previous_time=previous_timestamp,
                status=status,
                time=int(round(time.time() * TIME_SCALE))
            )
            if self.continue_flag != True:
                info('[Planner] - [Stop Scan] ...')
                break
            if result:
                Hub.report_demand(InstructionType.WORKER, self.queue, result)

    def start(self, restart=True):
        debug('[Planner] - [Start(restart=%s)] - %s' % (restart, self))
        if self.thread:
            if restart:
                self.stop()
                self.start(restart=False)
            else:
                raise Exception('thread already exist')
        else:
            self.continue_flag = True
            self.thread = start_daemon_thread(self.plan)

    def stop(self, blocking=True):
        debug('[Planner] - [Stop] - %s' % self)
        if self.thread is None:
            return
        else:
            self.continue_flag = False
            if blocking:
                self.thread.join(timeout=10)
                self.thread = None
            else:
                start_daemon_thread(self.force_quit, args=(self.thread,))

    def force_quit(self, thread):
        time.sleep(HEARTBEAT_INTERVAL * 2)
        terminate_thread(thread)
        self.thread = None


def schedule_planner(app, queue, strategy_name):
    debug('Schedule planner : queue %s:%s', queue, strategy_name)
    planner = Planner.get_instance(app, queue, strategy_name)
    if planner.alive:
        planner.restart()
    else:
        planner.start()
