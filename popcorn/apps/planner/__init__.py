import threading
import time
from collections import defaultdict
from popcorn.apps.base import BaseApp
from popcorn.apps.constants import TIME_SCALE
from popcorn.apps.hub import Hub
from popcorn.apps.hub.order.instruction import InstructionType
from popcorn.apps.planner.strategy import SimpleStrategy
from popcorn.apps.utils.broker_util import taste_soup
from popcorn.rpc.pyro import PyroClient
from popcorn.utils import get_log_obj, get_pid, start_daemon_thread, terminate_thread, call_callable


debug, info, warn, error, critical = get_log_obj(__name__)
STRATEGY_MAP = {
    SimpleStrategy.name: 'popcorn.apps.planner.strategy.simple:SimpleStrategy'
}


class RegisterPlanner(BaseApp):
    def __init__(self, app, **kwargs):
        self.app = app or self.app
        super(RegisterPlanner, self).init(**kwargs)
        self.rpc_client = PyroClient(self.app.conf['HUB_IP'])

    def start(self):
        self.rpc_client.call('popcorn.apps.planner.commands:start_planner',
                              app=self.app,
                              queue=self.queue,
                              strategy_name=self.strategy_name)

    def setup_defaults(self, loglevel=None, logfile=None, queue=None, strategy=None, **_kw):
        super(RegisterPlanner, self).setup_defaults(loglevel=None, logfile=None, queue=None, strategy=None, **_kw)
        self.queue = self._getopt('queue', queue)
        self.strategy_name = self._getopt('strategy', strategy)


class PlannerPool(object):
    pool = defaultdict(lambda: None)

    @classmethod
    def get_or_create_planner(cls, app, queue, strategy_name):
        planner = cls.pool.get(queue)
        if planner is None:
            planner = Planner(app, queue, strategy_name)
            cls.pool[queue] = planner
        return planner

    @classmethod
    def stats(cls):
        return {q: str(v) for q, v in cls.pool.items()}


class Planner(object):

    def __init__(self, app, queue, strategy_name):
        self.app = app or self.app
        self.queue = queue
        self.thread = None
        self.load_strategy(strategy_name)
        self.__shutdown = threading.Event()

    def __repr__(self):
        return 'Queue: %s Strategy: %s' % (self.queue, self.strategy.name)

    @property
    def alive(self):
        return self.thread is not None and self.thread.is_alive()

    def start(self):
        if self.alive:
            debug('[Planner] - [Already Start] - %s' % self)
        else:
            self.__shutdown.clear()
            self.thread = start_daemon_thread(self.loop)
            while not self.thread.is_alive():
                continue
            debug('[Planner] - [Start] - %s' % (self))

    def stop(self):
        if not self.alive:
            return
        self.__shutdown.set()
        if self.wait_condition_till_timeout(self.alive, 10):
            self.force_quit()
        if self.wait_condition_till_timeout(self.alive, 10):
            raise PlannerException('Could not stop planner %s' % self.queue)
        debug('[Planner] - [Stop] - %s' % self)

    def force_quit(self):
        start_daemon_thread(terminate_thread, args=(self.thread,))

    def load_strategy(self, strategy_name):
        debug('[Planner] - [Load Strategy] - %s' % strategy_name)
        self.strategy = call_callable(STRATEGY_MAP[strategy_name])

    def loop(self, condition=lambda: True):
        LOOP_INTERVAL = 5
        status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
        while condition and not self.__shutdown.isSet():
            debug('[Planner] - [Send] - [HeartBeat] : %s. PID: %s', self, get_pid())
            previous_timestamp = int(round(time.time() * TIME_SCALE))
            previous_status = status
            time.sleep(LOOP_INTERVAL)
            status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
            result = self.strategy.apply(
                previous_status=previous_status,
                previous_time=previous_timestamp,
                status=status,
                time=int(round(time.time() * TIME_SCALE))
            )
            if result:
                Hub.report_demand(InstructionType.WORKER, self.queue, result)
        info('[Planner] - [Stop] - %s' % self.queue)

    def wait_condition_till_timeout(self, condition, seconds):
        timeout_start = time.time()
        while condition and time.time() < timeout_start + seconds:
            continue
        return condition
