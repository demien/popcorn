import time
import random
from math import ceil
from time import sleep
from popcorn.apps.planner.strategy.base import BaseStrategy


def taste_soup():
    return random.randrange(0, 1000)


class SimpleStrategy(BaseStrategy):

    def __init__(self, result_queue):
        super(SimpleStrategy, self).__init__()
        self.result_queue = result_queue

    def apply(self):
        self.taste()

    def taste(self, salt=5, interval=2, threshold=10):
        scale = 10000
        while True:
            last_taste_at = int(round(time.time() * scale))
            last_soup_taste = taste_soup()

            sleep(interval)
            taste_at = int(round(time.time() * scale))
            soup_taste = taste_soup()

            delta = (soup_taste - last_soup_taste) * scale / (taste_at - last_taste_at)
            salt_add = salt * ceil(delta / threshold) if delta > 0 else 0
            print last_soup_taste, soup_taste
            print delta, salt_add

            self.result_queue.put(salt_add)
