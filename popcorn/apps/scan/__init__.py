from celery import bootsteps
from popcorn.rpc.pyro import PyroClient, HUB_PORT
from popcorn.utils import red, yellow, white, green, blue


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
    BREAK = '\n'

    def __init__(self, app, target):
        self.app = app or self.app
        self.target = target
        self.rpc_client = PyroClient(self.app.conf['HUB_IP'], HUB_PORT)

    def start(self):
        re = self.rpc_client.call('popcorn.apps.hub.commands.scan', target=self.target)
        print white(self.HEADER_PATTERN % self.target)
        if self.target == ScanTarget.MACHINE:
            self._scan_machine(re)
        if self.target == ScanTarget.PLANNER:
            self._scan_planner(re)
        print white(self.FOOTER)

    def _scan_machine(self, machines):
        for id, machine in machines.iteritems():
            snapshot = machine.snapshots[-1]
            print self.TAB, self.SEPERATOR
            print self.TAB, '%s: %s' % (white('ID'), blue(id))
            print self.TAB, '%s: %s' % (white('Update Time'), snapshot['time'])
            print self.TAB, '%s: %s' % (white('Healthy'), green(snapshot['healthy']) if snapshot['healthy'] else red(snapshot['healthy']))
            if snapshot.get('labels', None) is not None:
                print self.TAB, '%s: %s' % (white('Labels'), ','.join(snapshot['labels']))
            for key, value in snapshot['extra'].iteritems():
                if value:
                    print self.TAB, '%s:' % white(key)
                    for _key, _value in value.iteritems():
                        print self.TAB, self.TAB, '%s: %s' % (white(_key), _value)
            print snapshot['hardware']
            print self.BREAK

    def _scan_planner(self, planners):
        for queue, strategy in planners.iteritems():
            print 'Queue: %s %s Stretegy: %s' % (queue, self.TAB, strategy)
