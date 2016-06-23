from popcorn.apps.guard.machine import Machine
from popcorn.rpc.pyro import PyroClient
import time
import subprocess
import socket
from celery import bootsteps
from popcorn.rpc.pyro import RPCClient
import psutil
from collections import defaultdict
from popcorn.apps.utils import taste_soup


class Guard(object):

    class Blueprint(bootsteps.Blueprint):
        """Hub bootstep blueprint."""
        name = 'Guard'
        default_steps = set([
            'popcorn.rpc.pyro:RPCClient',
            'popcorn.apps.guard:Register',
            'popcorn.apps.guard:Loop',
        ])

    def __init__(self, app):
        self.app = app or self.app
        self.steps = []
        self.processes = defaultdict(list)
        self.machine = Machine()
        self.blueprint = self.Blueprint(app=self.app)
        self.blueprint.apply(self)

    @property
    def id(self):
        return self.machine.id

    def start(self):
        self.blueprint.start(self)

    def loop(self, rpc_client):
        while True:
            try:
                order = self.heartbeat(rpc_client)
                if order:
                    print '[Guard] get order: %s' % str(order)
                    self.follow_order(order)
            except Exception:
                import traceback
                traceback.print_exc()
            finally:
                time.sleep(5)

    def heartbeat(self, rpc_client):
        snapshot = self.machine.snapshot()
        return rpc_client.start_with_return('popcorn.apps.hub:hub_guard_heartbeat', machine=self.machine)

    def follow_order(self, order):
        print '[Guard] follow order:'
        # for queue, worker_number in order.iteritems():
        #     print '[Guard] Queue[%s], Workers [%2d]' % (queue, worker_number)
        #     self.update_worker(queue, worker_number)

    def update_worker(self, queue, worker_number):
        plist = self.processes[queue]
        delta = worker_number - len(plist)
        if delta > 0:
            self.add_worker(queue, delta)

    def clear_worker(self):
        for queue in self.processes.keys():
            if self.qsize(queue) == 0:
                print '[Guard] Clear Work For Empyt Queue'
                self.add_worker(queue, number=-100)

    def add_worker(self, queue, number=1):
        print '[Guard] Queue[%s], %d Workers' % (queue, number)
        if number > 0:
            for _ in range(number):
                if self.machine.health:
                    print '[Guard] Exec command: %s' % str(['celery', 'worker', '-Q', queue])
                    self.processes[queue].append(subprocess.Popen(['celery', 'worker', '-Q', queue]))
                    time.sleep(1)
                else:
                    print '[Guard] not more resource on this machine'
        elif number < 0:
            for _ in range(abs(number)):
                plist = self.processes[queue]
                if len(plist) >= 1:
                    p = plist.pop()
                    # p.send_signal(2)  # send Ctrl + C to subprocess
                    p.terminate()  # send Ctrl + C to subprocess
                    p.wait()  # wait this process quit
                else:
                    # no more workers
                    return

    def qsize(self, queue):
        return taste_soup(queue, self.app.conf['BROKER_URL'])

    @property
    def worker_stats(self):
        return {queue: len(plist) for queue, plist in self.processes.items()}


class Register(bootsteps.StartStopStep):
    requires = (RPCClient,)

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, p):
        return True

    def create(self, p):
        p.rpc_client.start('popcorn.apps.hub:hub_guard_register', machine=p.machine)
        return self

    def start(self, p):
        pass

    def stop(self, p):
        print 'in stop'

    def terminate(self, p):
        print 'in terminate'


class Loop(bootsteps.StartStopStep):
    requires = (Register,)

    def __init__(self, p, **kwargs):
        pass

    def include_if(self, p):
        return True

    def create(self, p):
        return self

    def start(self, p):
        p.loop(p.rpc_client)
        pass

    def stop(self, p):
        print 'in stop'

    def terminate(self, p):
        print 'in terminate'
