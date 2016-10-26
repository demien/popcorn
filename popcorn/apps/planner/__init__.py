import threading
import time
from collections import defaultdict
from popcorn.apps.base import BaseApp
from popcorn.apps.constants import TIME_SCALE
from popcorn.apps.exceptions import PlannerException
from popcorn.apps.hub import Hub
from popcorn.apps.hub.order.instruction import InstructionType
from popcorn.apps.planner.strategy import SimpleStrategy
from popcorn.apps.utils.broker_util import taste_soup
from popcorn.rpc.pyro import PyroClient, HUB_PORT
from popcorn.utils import (
    get_log_obj, get_pid, start_daemon_thread, terminate_thread, call_callable, wait_condition_till_timeout
)


debug, info, warn, error, critical = get_log_obj(__name__)
STRATEGY_MAP = {
    SimpleStrategy.name: 'popcorn.apps.planner.strategy.simple:SimpleStrategy'
}


class RegisterPlanner(BaseApp):
    def __init__(self, app, **kwargs):
        self.app = app or self.app
        super(RegisterPlanner, self).init(**kwargs)
        self.rpc_client = PyroClient(self.app.conf['HUB_IP'], HUB_PORT)

    def start(self):
        self.rpc_client.call('popcorn.apps.planner.commands:start_planner',
                              app=self.app,
                              queue=self.queue,
                              strategy_name=self.strategy_name,
                              labels=self.labels)

    def setup_defaults(self, **_kw):
        super(RegisterPlanner, self).setup_defaults(**_kw)
        self.queue = _kw.get('queue')
        self.strategy_name = _kw.get('strategy')
        self.labels = _kw.get('labels', '').split(',')


class PlannerPool(object):
    pool = defaultdict(lambda: None)

    @classmethod
    def reset(cls):
        cls.pool.clear()

    @classmethod
    def get_or_create_planner(cls, queue, app, strategy_name, labels):
        planner = cls.get_planner(queue)
        if planner is None:
            planner = PlannerPool.create_planner(app, queue, strategy_name, labels)
            cls.pool[queue] = planner
        return planner

    @classmethod
    def get_planner(cls, queue):
        return cls.pool.get(queue)

    @staticmethod
    def create_planner(app, queue, strategy_name, labels):
        return Planner(app, queue, strategy_name, labels)

    @classmethod
    def stats(cls):
        return {q: str(v) for q, v in cls.pool.items()}

    @classmethod
    def stop(cls):
        for planner in cls.pool.values():
            planner.stop()
            cls.reset()


class Planner(object):

    def __init__(self, app, queue, strategy_name, labels=[]):
        self.app = app or self.app
        self.queue = queue
        self.thread = None
        self.load(strategy_name, labels)
        self.__shutdown = threading.Event()
        self.loop_interval = 5

    def __repr__(self):
        return 'Queue: %s. Strategy: %s. Labels: %s.' % (self.queue, self.strategy.name, ','.join(self.labels) or None)

    def __eq__(self, another):
        return self.queue == another.queue and self.strategy == another.strategy

    @property
    def alive(self):
        return self.thread is not None and self.thread.is_alive()

    def is_alive(self):
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
        if wait_condition_till_timeout(self.is_alive, 10):
            self.force_quit()
        if wait_condition_till_timeout(self.is_alive, 10):
            raise PlannerException('Could not stop planner %s' % self.queue)
        debug('[Planner] - [Stop] - %s' % self)

    def force_quit(self):
        start_daemon_thread(terminate_thread, args=(self.thread,))

    def load(self, strategy_name, labels):
        self.strategy = call_callable(STRATEGY_MAP[strategy_name])
        self.labels = labels
        debug('[Planner] - [Load] - %s' % self)

    def loop(self, condition=lambda: True):
        status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
        while condition and not self.__shutdown.isSet():
            try:
                debug('[Planner] - [Send] - [HeartBeat] : %s PID: %s', self, get_pid())
                previous_timestamp = int(round(time.time() * TIME_SCALE))
                previous_status = status
                time.sleep(self.loop_interval)
                status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
                result = self.strategy.apply(
                    previous_status=previous_status,
                    previous_time=previous_timestamp,
                    status=status,
                    time=int(round(time.time() * TIME_SCALE))
                )
                if result:
                    Hub.report_demand(InstructionType.WORKER, self.queue, result)
            except Exception as e:
                error('[Planner] - [Exception] - [Loop] : %s. PID: %s', e.message, get_pid())
        info('[Planner] - [Stop] - %s' % self.queue)
