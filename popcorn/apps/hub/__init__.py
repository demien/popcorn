from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from collections import defaultdict
import os


class Machine(object):
    RECORD_NUMBER = 100

    _CPU_WINDOW_SIZE = 5
    _CPU_THS = 10  # 10 percent
    _MEMORY_THS = 500 * 1024 ** 2  # if remain memeory < this number , not start more worker

    def __init__(self, id):
        self.id = id
        self._original_stats = []
        self._plan = defaultdict(int)

    def update_stats(self, stats):
        print '[Machine] %s cpu:%s , memory:%s MB' % (self.id, self.cpu, self.memory / 1024 ** 2)
        self._original_stats.append(stats)
        if len(self._original_stats) > self.RECORD_NUMBER:
            self._original_stats.pop(0)

    @property
    def memory(self):
        if self._original_stats:
            return self._original_stats[-1]['memory'].available
        else:
            return self._MEMORY_THS + 100

    @property
    def cpu(self):
        if len(self._original_stats) >= self._CPU_WINDOW_SIZE:
            # return sum([i['cpu'].idle for i in self._original_stats[-self._CPU_WINDOW_SIZE:]]) / float(
            #     self._CPU_WINDOW_SIZE)
            if sum([1 for i in self._original_stats[-self._CPU_WINDOW_SIZE:] if i['cpu'].idle < 5]) > 1:
                return 100
            else:
                return 0
        else:
            return 50

    @property
    def health(self):
        return self.cpu >= self._CPU_THS and self.memory >= self._MEMORY_THS

    def get_worker_number(self, queue):
        return self._plan[queue]

    def current_worker_number(self, queue):
        return 1

    def plan(self, *queues):
        return {queue: self.get_worker_number(queue) for queue in queues}
        # import random
        # return {'pop': random.randint(1, 6)}

    # def update_plan(self, queue, worker_number):
    #     if self.health:
    #         support = self.memory / 100 * 1024 ** 2
    #         if worker_number <= support:
    #             self._plan[queue] = worker_number
    #         else:
    #             self._plan[queue] = support
    #     print '[Machine] %s take %d workers' % (self.id, self._plan.get(queue, 0))
    #     return self._plan[queue]  # WARNING should always return workers you take in

    def add_plan(self, queue, worker_number):
        self._plan[queue] += worker_number

    def clear_plan(self):
        self._plan = defaultdict(int)


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
        # print '[Hub] guard stats: %s, %s' % (id, stats)
        try:
            machine = Hub.MACHINES.get(id, Machine(id))
            Hub.MACHINES[id] = machine
            machine.update_stats(stats)
            print "[hub] Queue plan:", Hub.PLAN
            for queue, worker_number in Hub.PLAN.iteritems():
                Hub.PLAN[queue] = Hub.load_balancing(queue, worker_number)
            machine_plan = machine.plan(*Hub.PLAN.keys())
            machine.clear_plan()
            print "[hub] Plan machine:", id, machine_plan
            return machine_plan
        except Exception:
            import traceback
            traceback.print_exc()
        return {}

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
    def load_balancing(queue, worker_number):
        remain_worker = 0
        while worker_number:
            for id, machine in Hub.MACHINES.items():
                if machine.health:
                    machine.add_plan(queue, 1)
                    worker_number -= 1
                else:
                    print "Machine >>> unhealth"
            if not [machine for machine in Hub.MACHINES.values() if machine.health]:
                print '[Hub] warning , remain %d workers' % worker_number
                remain_worker = worker_number
        for machine in Hub.MACHINES.values():
            print '[Machine] %s take %d workers' % (machine.id, machine._plan[queue])
        return remain_worker

            # if worker_number > 0:
            #     print '[Hub] warning , remain %d workers' % worker_number
            #     return
            # for id, machine in Hub.MACHINES.items():
            #     worker_number -= machine.update_plan(queue, worker_number)
            # if worker_number > 0:
            #     print '[Hub] warning , remain %d workers' % worker_number


def hub_send_order(id, stats):
    """
    :param id:
    :param stats: dict contains memory & cpu use
    :return:
    """
    return Hub.send_order(id, stats=stats)


def hub_set_plan(plan=None):
    return Hub.set_plan(plan)


def hub_enroll(id):
    return Hub.enroll(id)
