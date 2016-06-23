import math
import os
import traceback
from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from collections import defaultdict
from popcorn.apps.guard.machine import Machine
from popcorn.apps.hub.order.instruction import Instruction
from state import DEMAND, PLAN, MACHINES, add_demand, remove_demand, add_plan, pop_order, update_machine


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
            print "[Hub] Analyze Demand: %s", DEMAND
            for queue, worker_cnt in DEMAND.iteritems():
                if Hub.load_balancing(queue, worker_cnt):
                    remove_demand(queue)
            order = pop_order(machine.id)
            return order
        except Exception as e:
            traceback.print_exc()
            print e
            return None
        return None

    @staticmethod
    def report_demand(type, instruction):
        instruction = Instruction.create(type, instruction)
        current_worker_cnt = Hub.PLAN.get(instruction.queue)
        new_worker_cnt = instruction.operator.apply(current_worker_cnt, instruction.worker_cnt)
        add_demand[instruction.queue] = new_worker_cnt

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
        worker_per_machine = math.ceil(worker_cnt / len(healthy_machines))
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
    return Hub.guard_heartbeat(id, stats=stats)


def hub_report_demand(type, instruction):
    return Hub.report_demand(type, instruction)


def hub_guard_register(machine):
    return Hub.guard_register(machine)
