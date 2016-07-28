import math
import os
import json
import traceback

from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from collections import defaultdict

from popcorn.apps.guard.machine import Machine
from popcorn.apps.hub.order.instruction import Instruction
from popcorn.rpc.pyro import RPCServer as _RPCServer
from popcorn.utils.log import get_log_obj

from state import (
    DEMAND, PLAN, MACHINES, PLANNERS, add_demand, remove_demand, add_plan, pop_order, update_machine, get_worker_cnt)


debug, info, warn, error, critical = get_log_obj(__name__)


class Hub(object):
    class Blueprint(bootsteps.Blueprint):
        """Hub bootstep blueprint."""
        name = 'Hub'
        default_steps = set([
            'popcorn.apps.hub:LoadPlanners',
            'popcorn.apps.hub:RPCServer',  # fix me, dynamic load rpc portal
        ])

    def __init__(self, app, hostname=None, **kwargs):
        self.app = app or self.app
        self.steps = []
        self.setup_defaults(**kwargs)
        self.setup_instance(**kwargs)

        self.blueprint = self.Blueprint(app=self.app)
        self.blueprint.apply(self, **kwargs)

    def setup_defaults(self, loglevel=None, logfile=None, **_kw):
        self.loglevel = self._getopt('log_level', loglevel)
        self.logfile = self._getopt('log_file', logfile)

    def _getopt(self, key, value):
        if value is not None:
            return value
        return self.app.conf.find_value_for_key(key, namespace='celeryd')

    def setup_instance(self, **kwargs):
        self.no_color = None
        self.on_init_blueprint(**kwargs)

    def on_init_blueprint(self, **kwargs):
        self._custom_logging = self.setup_logging()

    def setup_logging(self, colorize=None):
        if colorize is None and self.no_color is not None:
            colorize = not self.no_color
        return self.app.log.setup(self.loglevel, self.logfile, redirect_stdouts=False, colorize=colorize)

    def start(self):
        self.blueprint.start(self)

    @staticmethod
    def guard_heartbeat(machine):
        try:
            update_machine(machine)
            Hub.analyze_demand()
            return pop_order(machine.id)
        except Exception as e:
            traceback.print_exc();
            print e
            return None

    @staticmethod
    def analyze_demand():
        if not DEMAND:
            return
        # print "[Hub: Guard Heartbeat] Analyze Demand: %s" % json.dumps(DEMAND)
        success_queues = []
        for queue, worker_cnt in DEMAND.iteritems():
            if Hub.load_balancing(queue, worker_cnt):
                success_queues.append(queue)
        for queue in success_queues:
            remove_demand(queue)

    @staticmethod
    def report_demand(type, cmd):
        instruction = Instruction.create(type, cmd)
        current_worker_cnt = get_worker_cnt(instruction.queue)
        new_worker_cnt = instruction.operator.apply(current_worker_cnt, instruction.worker_cnt)
        add_demand(instruction.queue, new_worker_cnt)

    @staticmethod
    def guard_register(machine):
        info('[Guard] - [Register] - %s' % machine.id)
        update_machine(machine)

    @staticmethod
    def load_balancing(queue, worker_cnt):
        healthy_machines = [machine for machine in MACHINES.values() if machine.healthy]
        if not healthy_machines:
            print '[Hub] - [Warning] - No Healthy Machines!'
            return False
        worker_per_machine = int(math.ceil(worker_cnt / len(healthy_machines)))
        for machine in healthy_machines:
            print '[Hub] - [Load Balance] - {%s} take %d workers on #%s' % (machine.id, worker_per_machine, queue)
            add_plan(queue, machine.id, worker_per_machine)
        return True

    @staticmethod
    def scan(target):
        from popcorn.apps.scan import ScanTarget
        if target == ScanTarget.MACHINE:
            return dict(MACHINES)
        if target == ScanTarget.PLANNER:
            return dict(PLANNERS)


class LoadPlanners(bootsteps.StartStopStep):

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, p):
        return True

    def create(self, p):
        return self

    def start(self, p):
        from popcorn.apps.planner import Planner
        for queue, strategy in p.app.conf.get('DEFAULT_QUEUE', {}).iteritems():
            Planner(p.app, queue=queue, strategy_name=strategy).start()

    def stop(self, p):
        print 'in stop'

    def terminate(self, p):
        print 'in terminate'


class RPCServer(_RPCServer):
    requires = (LoadPlanners,)


def hub_guard_heartbeat(machine):
    """
    :param id:
    :param stats: dict contains memory & cpu use
    :return:
    """
    return Hub.guard_heartbeat(machine)


def hub_report_demand(type, cmd):
    return Hub.report_demand(type, cmd)


def hub_guard_register(machine):
    return Hub.guard_register(machine)

def hub_scan(target):
    return Hub.scan(target)
