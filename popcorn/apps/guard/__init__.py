from popcorn.rpc.pyro import PyroClient
import time


class Guard(object):

    def start(self):
        print 'start guard'
        while True:
            self.collect_machine_info()
            construction = self.send_to_hub()
            print construction
            time.sleep(2)

    def send_to_hub(self):
        client = PyroClient()
        return client.start('popcorn.apps.hub:hub_instruction')

    def collect_machine_info(self):
        print 'collect_machine_info'
