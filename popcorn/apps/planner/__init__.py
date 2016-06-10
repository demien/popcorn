import time
import logging
from multiprocessing import Process

from popcorn.apps.constants import TIME_SCALE, INTERVAL
from popcorn.apps.hub import Hub
from popcorn.apps.utils.broker_util import taste_soup
from popcorn.rpc.pyro import PyroClient

logger = logging.getLogger(__name__)


STRATEGY_MAP = {
    'simple': 'popcorn.apps.planner.strategy.SimpleStrategy:SimpleStrategy'
}


class Planner(object):

    def __init__(self, queue, strategy, **kwargs):
        self.rpc_client = PyroClient()
        self.task_queue = queue
        path, class_name = STRATEGY_MAP.get(strategy).split(':', 1)
        try:
            strategy_class = getattr(__import__(path, fromlist=[class_name]), class_name)
        except ImportError as ie:
            logger.error("Can not import {0} from {1}, Tracker: {2}".format(
                path, class_name, ie.message))
            raise

        self.strategy = strategy_class()
        process = Process(target=self.plan)
        process.start()
        print "Process started"

    def plan(self):
        while True:
            previous_timestampe = int(round(time.time() * TIME_SCALE))
            previous_status = taste_soup(self.task_queue)
            time.sleep(INTERVAL)
            timestampe = int(round(time.time() * TIME_SCALE))
            status = taste_soup(self.task_queue)
            result = self.strategy.apply(
                previous_status=previous_status,
                previous_time=previous_timestampe,
                status=status,
                time=timestampe
            )
            plan = {self.task_queue: result}
            print 'plan ', plan
            self.rpc_client.start('popcorn.apps.hub:hub_set_plan', plan=plan)
