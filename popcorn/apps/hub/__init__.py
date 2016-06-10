from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE


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

    def foo(self):
        while True:
            print 'in foo'
