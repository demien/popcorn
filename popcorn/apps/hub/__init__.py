from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from collections import defaultdict
import os


class Hub(object):

    class Blueprint(bootsteps.Blueprint):
        """Hub bootstep blueprint."""
        name = 'Hub'
        default_steps = set([
            'popcorn.rpc.pyro:RPCServer',  # fix me, dynamic load rpc portal
        ])

    PLAN = defaultdict(int)
    MACHINES = []

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        self.steps = []
        self.blueprint = self.Blueprint(app=self.app)
        self.blueprint.apply(self, **kwargs)

    def start(self):
        self.blueprint.start(self)

    @staticmethod
    def send_order(id):
        order = defaultdict(int)
        for queue, task in Hub.PLAN.iteritems():
            order[queue] = int(task / len(Hub.MACHINES))
        return order

    @staticmethod
    def set_plan(plan):
        Hub.PLAN.update(plan)

    @staticmethod
    def enroll(id):
        print '[Hub] new guard enroll: %s' % id
        Hub.MACHINES.append(id)


def hub_send_order(id):
    return Hub.send_order(id)

def hub_set_plan(plan=None):
    return Hub.set_plan(plan)

def hub_enroll(id):
    return Hub.enroll(id)
