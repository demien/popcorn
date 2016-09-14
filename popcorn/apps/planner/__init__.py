import ctypes
import threading
import time
from celery.utils.imports import instantiate

from popcorn.apps.base import BaseApp
from popcorn.apps.constants import TIME_SCALE
from popcorn.apps.hub import Hub
from popcorn.apps.hub.order.instruction import InstructionType
from popcorn.apps.utils.broker_util import taste_soup
from popcorn.rpc.pyro import RPCClient
from popcorn.utils import get_log_obj, get_pid

debug, info, warn, error, critical = get_log_obj(__name__)
HEARTBEAT_INTERVAL = 5

STRATEGY_MAP = {
    'simple': 'popcorn.apps.planner.strategy.simple:SimpleStrategy'
}


def start_back_ground_task(target, args=()):
    debug('[BG] Tast New Thread %r', target)
    thread = threading.Thread(target=target, args=args)
    thread.daemon = True
    thread.start()
    return thread


def terminate_thread(thread):
    """Terminates a python thread from another thread.

    :param thread: a threading.Thread instance
    """
    if not thread.isAlive():
        return

    exc = ctypes.py_object(SystemExit)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(thread.ident), exc)
    if res == 0:
        raise ValueError("nonexistent thread id")
    elif res > 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread.ident, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class RegisterPlanner(BaseApp):
    def __init__(self, app, **kwargs):
        self.app = app or self.app
        self.steps = []
        self.setup_defaults(**kwargs)
        self.setup_instance(**kwargs)
        super(RegisterPlanner, self).init(**kwargs)
        RPCClient(None).create(self)  # this operation will bind rpc_client on self

    def start(self):
        self.rpc_client.call('popcorn.apps.planner:schedule_planner',
                              app=self.app,
                              queue=self.queue,
                              strategy_name=self.strategy_name)

    def setup_defaults(self, loglevel=None, logfile=None, queue=None, strategy=None, **_kw):
        self.queue = self._getopt('queue', queue)
        self.strategy_name = self._getopt('strategy', strategy)
        self.loglevel = self._getopt('log_level', loglevel)
        self.logfile = self._getopt('log_file', logfile)


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
        self.steps = []
        self.queue = queue
        self.strategy_name = strategy_name
        strategy_cls = STRATEGY_MAP[strategy_name]
        self.strategy = instantiate(strategy_cls)
        self.continue_flag = True
        info('[Planner] - [Register] - Queue: %s, Strategy: %s' % (self.queue, self.strategy_name))
        self.thread = None

    @property
    def alive(self):
        return self.thread is not None

    def restart(self):
        self.start(restart=True)

    def __str__(self):
        return '<Planner %s:%s> (Alive:%s)' % (self.strategy_name, self.queue, self.alive)

    def plan(self):
        status = taste_soup(self.queue, self.app.conf['BROKER_URL'])
        while True:
            debug('[Planner] - [HeartBeat] - %s , Thread %s. PID: %s', self, threading.current_thread(), get_pid())
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
            self.thread = start_back_ground_task(self.plan)

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
                start_back_ground_task(self.force_quit, args=(self.thread,))

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
