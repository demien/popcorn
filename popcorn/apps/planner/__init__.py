import time
import logging
from multiprocessing import Process

from popcorn.apps.constants import TIME_SCALE, INTERVAL
from popcorn.apps.utils.broker_util import taste_soup

logger = logging.getLogger(__name__)


STRATEGY_MAP = {
    'simple': 'popcorn.apps.planner.strategy.SimpleStrategy:SimpleStrategy'
}


class Planner(object):

    def __init__(self, queue, strategy, **kwargs):
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
            timestampe = int(round(time.time() * TIME_SCALE))
            time.sleep(INTERVAL)
            status = taste_soup()
            time.sleep(INTERVAL)
            self.strategy.apply(status=status, time=timestampe)

    def send_plan(self):
        pass
