from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from collections import defaultdict
import os


class Machine(object):
    RECORD_NUMBER = 100

    def __init__(self, id):
        self.id = id
        self.stats = []
        self._plan = {
        }

    def update_stats(self, stats):
        self.stats.append(stats)
        if len(self.stats) > self.RECORD_NUMBER:
            self.stats.pop(0)

    @property
    def memory(self):
        return self.stats[-1]['memory']['available']

    def health(self):
        return True

    def get_worker_number(self, queue):
        return self._plan.get(queue, 0)

    def current_worker_number(self, queue):
        return 1

    def plan(self, *queues):
        # return {queue: self.get_worker_number(queue) for queue in queues}
        import random
        return {'pop': random.randint(1, 6)}

    def update_plan(self, queue, worker):
        self._plan[queue] = worker


class Task(object):
    memory_consume = 0

    def __init__(self):
        pass

        # @property
        # def memory_consume(self):
        #     """
        #     :return: int, maximum bytes this task need
        #     50MB for current demo
        #     """
        #     return 50 * 1024 ** 2


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
        machine = Hub.MACHINES.get(id, Machine(id))
        Hub.MACHINES[id] = machine
        machine.update_stats(stats)
        for queue, worker_number in Hub.PLAN.iteritems():
            Hub.weighted_worker_scheduling(worker_number, queue)
        machine_plan = machine.plan(*Hub.PLAN.keys())
        print "Debug >>> machine:", id, machine_plan
        return machine_plan

    def get_worker_number(self, queue):
        # get numbers we need
        return 10

    @staticmethod
    def clear_plan():
        Hub.PLAN = defaultdict(int)

    @staticmethod
    def set_plan(plan):
        Hub.PLAN.update(plan)

    @staticmethod
    def enroll(id):
        print '[Hub] new guard enroll: %s' % id
        Hub.MACHINES[id] = Machine(id)

    @staticmethod
    def balancing(queue, worker_number):
        # update machine's work number
        pass

    @staticmethod
    def weighted_worker_scheduling(worker_number, queue):
        memory = 0
        for id, machine in Hub.MACHINES.items():
            # TODO: calculating here
            machine.update_plan(queue, 5)


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
