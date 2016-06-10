from popcorn.rpc.pyro import PyroClient
import time


class Guard(object):

    def start(self):
        while True:
            self.collect_machine_info()
            order = self.get_order()
            print '[Guard] get order: %s' % order
            time.sleep(2)

    def get_order(self):
        client = PyroClient()
        return client.start('popcorn.apps.hub:hub_send_order')

    def collect_machine_info(self):
        print '[Guard] collect info:  CUP 90%'
