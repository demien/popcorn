from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from collections import defaultdict
import os

PLAN = defaultdict(int)
MACHINES = []


class Hub(object):

    class Blueprint(bootsteps.Blueprint):

        """Hub bootstep blueprint."""
        name = 'Hub'
        default_steps = set([
            # 'popcorn.apps.hub.components:PlannerServer',
            'popcorn.apps.hub.components:RPCServer',
        ])

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        self.steps = []
        self.blueprint = self.Blueprint(app=self.app)
        self.blueprint.apply(self, **kwargs)

    def start(self):
        self.blueprint.start(self)

    @staticmethod
    def send_order(id):
        print os.getpid()
        print 'Before send order %s' % str(PLAN)

        order = defaultdict(int)
        for queue, task in PLAN.iteritems():
            order[queue] = int(task / len(MACHINES))
        return order

    @staticmethod
    def set_plan(plan):
        print os.getpid()
        print '[Hub] set plan: %s' % str(plan)
        PLAN.update(plan)
        print 'After set Plan %s' % str(PLAN)

    @staticmethod
    def enroll(id):
        print '[Hub] new guard enroll: %s' % id
        MACHINES.append(id)


def hub_send_order(id):
    return Hub.send_order(id)

def hub_set_plan(plan=None):
    return Hub.set_plan(plan)

def hub_enroll(id):
    return Hub.enroll(id)
