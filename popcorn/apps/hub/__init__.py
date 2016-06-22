import os
from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from collections import defaultdict
from popcorn.apps.guard.machine import Machine


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
        try:
            machine = Hub.MACHINES.get(id, Machine(id))
            Hub.MACHINES[id] = machine
            machine.update_stats(stats)
            if filter(lambda a: a != 0, Hub.PLAN.values()):
                print "[Hub] Analyze Queue plan:", Hub.PLAN
                for queue, worker_number in Hub.PLAN.iteritems():
                    Hub.PLAN[queue] = Hub.load_balancing(queue, worker_number)
                machine_plan = machine.plan(*Hub.PLAN.keys())
                print "[Hub] Create machine %s plan %s" % (str(id), str(machine_plan))
                machine.clear_plan()
                if filter(lambda a: a != 0, machine_plan.values()):
                    print "[hub] Plan machine:", id, machine_plan
                return machine_plan
        except Exception as e:
            import traceback
            traceback.print_exc()
            print e
            return {}
        return {}

    def get_worker_number(self, queue):
        # get numbers we need
        return 10

    @staticmethod
    def clear_plan():
        Hub.PLAN = defaultdict(int)

    @staticmethod
    def set_plan(plan):
        if filter(lambda a: a != 0, plan.values()):
            Hub.PLAN.update(plan)

    @staticmethod
    def enroll(id):
        print '[Hub] new guard enroll: %s' % id
        Hub.MACHINES[id] = Machine()

    @staticmethod
    def load_balancing(queue, worker_number):
        remain_worker = 0
        while worker_number > 0:
            for id, machine in Hub.MACHINES.items():
                if machine.health:
                    cnt = 1
                    if id == 'gyang-2.lan.appannie.com@192.168.1.243':
                        cnt = 2
                    machine.add_plan(queue, cnt)
                    worker_number -= cnt
                else:
                    print "[Machine] %s unhealth" % str(id)
            if not [machine for machine in Hub.MACHINES.values() if machine.health]:
                print '[Hub] warning , remain %d workers' % worker_number
                remain_worker = worker_number
                return remain_worker
        for machine in Hub.MACHINES.values():
            if machine._plan[queue]:
                print '[Machine] load balance: %s take %d workers' % (machine.id, machine._plan[queue])
        return remain_worker


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
