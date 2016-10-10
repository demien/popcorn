from celery import bootsteps
from popcorn.rpc.pyro import PyroClient, HUB_PORT


class ScanTarget(object):
    MACHINE = 'machine'
    PLANNER = 'planner'


class Scan(object):

    HEADER_PATTERN = '''
/**************************
 Scan %s Result:
'''
    FOOTER = '''
**************************/
    '''
    SEPERATOR = '-' * 20
    TAB = '\t'

    def __init__(self, app, target):
        self.app = app or self.app
        self.target = target
        self.rpc_client = PyroClient(self.app.conf['HUB_IP'], HUB_PORT)

    def start(self):
        re = self.rpc_client.call('popcorn.apps.hub.commands.scan', target=self.target)
        print self.HEADER_PATTERN % self.target
        if self.target == ScanTarget.MACHINE:
            self._scan_machine(re)
        if self.target == ScanTarget.PLANNER:
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
