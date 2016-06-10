import time
from math import ceil
from popcorn.apps.constants import TIME_SCALE
from popcorn.apps.planner.strategy.base import BaseStrategy
from popcorn.apps.utils.broker_util import taste_soup


class SimpleStrategy(BaseStrategy):

    def __init__(self, result_queue):
        super(SimpleStrategy, self).__init__()
        self.result_queue = result_queue

    def apply(self, **kwargs):
        previous_status = kwargs.get('status')
        previous_time = kwargs.get('time')
        self.taste(previous_status, previous_time)

    def taste(self, previous_status, previous_time, salt=5, threshold=10):
        taste_at = int(round(time.time() * TIME_SCALE))
        soup_taste = taste_soup()

        delta = (soup_taste - previous_status) * TIME_SCALE / (taste_at - previous_time)
        salt_add = salt * ceil(delta / threshold) if delta > 0 else 0
        print delta, salt_add

        self.result_queue.put(salt_add)
