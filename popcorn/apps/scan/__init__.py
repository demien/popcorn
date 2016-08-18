from celery import bootsteps
from popcorn.rpc.pyro import RPCClient


class ScanTarget(object):
    MACHINE = 'machine'
    PLANNER = 'planner'


class Scan(object):

    class Blueprint(bootsteps.Blueprint):
        """Hub bootstep blueprint."""
        name = 'Guard'
        default_steps = set([
            'popcorn.rpc.pyro:RPCClient',
            'popcorn.apps.scan:DoScan',
        ])

    def __init__(self, app, target):
        self.app = app or self.app
        self.target = target
        self.steps = []
        self.blueprint = self.Blueprint(app=self.app)
        self.blueprint.apply(self)

    def start(self):
        self.blueprint.start(self)


class DoScan(bootsteps.StartStopStep):
    requires = (RPCClient,)
    HEADER_PATTERN = '''
/**************************
 Scan %s Result:
'''
    FOOTER = '''
**************************/
    '''
    SEPERATOR = '-' * 20
    TAB = '\t'

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, p):
        return True

    def create(self, p):
        pass

    def start(self, p):
        re = p.rpc_client.start_with_return('popcorn.apps.hub:hub_scan', target=p.target)
        print self.HEADER_PATTERN % p.target
        if p.target == ScanTarget.MACHINE:
            self._scan_machine(re)
        if p.target == ScanTarget.PLANNER:
            self._scan_planner(re)
        print self.FOOTER

    def _scan_machine(self, machines):
        for id, machine in machines.iteritems():
            snapshot = machine.snapshots[-1]
            print self.TAB, self.SEPERATOR
            print self.TAB, 'id: %s' % id
            print self.TAB, 'update_time: %s' % snapshot['time']
            print self.TAB, 'healthy: %s' % snapshot['healthy']
            print snapshot['hardware']

    def _scan_planner(self, planners):
        for queue, strategy in planners.iteritems():
            print 'Queue: %s %s Stretegy: %s' % (queue, self.TAB, strategy)
