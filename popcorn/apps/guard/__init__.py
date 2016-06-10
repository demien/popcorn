from popcorn.rpc.pyro import PyroClient
import time
import subprocess


class Guard(object):

    def start(self):
        while True:
            self.collect_machine_info()
            order = self.get_order()
            print '[Guard] get order: %s' % str(order)
            self.follow_order(order)
            time.sleep(5)

    def get_order(self):
        client = PyroClient()
        return client.start('popcorn.apps.hub:hub_send_order')

    def collect_machine_info(self):
        print '[Guard] collect info:  CUP 90%'

    def follow_order(self, order):
        for queue, concurrency in order.iteritems():
            cmd = 'celery worker -Q %s -c %s' % (queue, concurrency)
            print '[Guard] exec command: %s' % cmd
            subprocess.Popen(cmd.split(' '))
