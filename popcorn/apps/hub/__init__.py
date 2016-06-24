import math
import os
import json
import traceback
from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from collections import defaultdict
from popcorn.apps.guard.machine import Machine
from popcorn.apps.hub.order.instruction import Instruction
from state import DEMAND, PLAN, MACHINES, add_demand, remove_demand, add_plan, pop_order, update_machine, \
    get_worker_cnt


class Hub(object):
    class Blueprint(bootsteps.Blueprint):
        """Hub bootstep blueprint."""
        name = 'Hub'
        default_steps = set([
            'popcorn.rpc.pyro:RPCServer',  # fix me, dynamic load rpc portal
        ])

    PLANNERS = {}

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        self.steps = []
        self.blueprint = self.Blueprint(app=self.app)
        self.blueprint.apply(self, **kwargs)

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
        print "[Hub: Guard Heartbeat] Analyze Demand: %s" % json.dumps(DEMAND)
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
        print '[Hub] new guard enroll: %s' % machine.id
        update_machine(machine)

    @staticmethod
    def load_balancing(queue, worker_cnt):
        healthy_machines = [machine for machine in MACHINES.values() if machine.healthy]
        if not healthy_machines:
            print '[Hub] warning no healthy machine'
            return False
        worker_per_machine = int(math.ceil(worker_cnt / len(healthy_machines)))
        for machine in healthy_machines:
            print '[Machine] load balance plan: %s take %d workers' % (machine.id, worker_per_machine)
            add_plan(queue, machine.id, worker_per_machine)
        return True


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
