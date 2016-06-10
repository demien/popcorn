import time
from math import ceil
from popcorn.apps.constants import TIME_SCALE
from popcorn.apps.planner.strategy.base import BaseStrategy
from popcorn.apps.utils.broker_util import taste_soup


class SimpleStrategy(BaseStrategy):

    def apply(self, **kwargs):
        previous_status = kwargs.get('previous_status')
        previous_time = kwargs.get('previous_time')
        status = kwargs.get('status')
        time = kwargs.get('time')
        return self.taste(previous_status, previous_time, status, time, threshold=100)

    def taste(self, previous_status, previous_time, current_status, current_time, salt=5, threshold=10):
        delta = (current_status - previous_status) * TIME_SCALE / (current_time - previous_time)
        salt_add = salt * ceil(delta / threshold) if delta > 0 else 0
        return int(salt_add)
