from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from collections import defaultdict
import os


class Machine(object):
    RECORD_NUMBER = 100

    def __init__(self, id):
        self.id = id
        self.stats = []

    def update_stats(self, stats):
        self.stats.append(stats)
        if len(self.stats) > self.RECORD_NUMBER:
            self.stats.pop(0)

    @property
    def memory(self):
        return self.stats[-1]['memory']['available']

    def health(self):
        return True


class Task(object):
    def __init__(self):
        pass

    @property
    def memory_consume(self):
        """
        :return: int, maximum bytes this task need
        50MB for current demo
        """
        return 50 * 1024 ** 2


class Hub(object):
    class Blueprint(bootsteps.Blueprint):
        """Hub bootstep blueprint."""
        name = 'Hub'
        default_steps = set([
            'popcorn.rpc.pyro:RPCServer',  # fix me, dynamic load rpc portal
        ])

    PLAN = defaultdict(int)
    MACHINES = {}

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        self.steps = []
        self.blueprint = self.Blueprint(app=self.app)
        self.blueprint.apply(self, **kwargs)

    def start(self):
        self.blueprint.start(self)

    @staticmethod
    def send_order(id, stats):
        order = defaultdict(int)
        machine = Hub.MACHINES.get(id, Machine(id))
        Hub.MACHINES[id] = machine
        machine.update_stats(stats)

        # TODO integration with YangGuang
        NEED_WORKER = 100

        for queue, task in Hub.PLAN.iteritems():
            order[queue] = int(task / len(Hub.MACHINES))
        return order

    @staticmethod
    def set_plan(plan):
        Hub.PLAN.update(plan)

    @staticmethod
    def enroll(id):
        print '[Hub] new guard enroll: %s' % id
        Hub.MACHINES[id] = Machine(id)

    @staticmethod
    def weighted_worker_scheduling(worker_number, task):
        memory = 0
        for id, machine in Hub.MACHINES.items():
            memory += machine.memory
        need_memory = worker_number * task.memory_consume


def hub_send_order(id, stats=None):
    """
    :param id:
    :param stats: dict contains memory & cpu use
    :return:
    """
    return Hub.send_order(id, stats=None)


def hub_set_plan(plan=None):
    return Hub.set_plan(plan)


def hub_enroll(id):
    return Hub.enroll(id)
