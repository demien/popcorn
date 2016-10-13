import unittest
import time
import logging
from mock import MagicMock
from popcorn.apps.exceptions import CouldNotStartException
from popcorn.apps.hub import Hub
from popcorn.apps.guard import Guard
from popcorn.apps.planner import PlannerPool
from popcorn.tests.mock_tool import App, Pool, PickableMock
from popcorn.utils import start_daemon_thread, wait_condition_till_timeout


logging.basicConfig(level=logging.ERROR)


class TestIntegration(unittest.TestCase):
    app = App()
    guard = Guard(app)
    guard.LOOP_INTERVAL = 0.1
    guard.pool = Pool()
    guard.pool.grow = PickableMock()
    hub = Hub(app)
    hub.LOOP_INTERVAL = 0.1
    queue = app.conf.config['DEFAULT_QUEUE'].keys()[0]
    strategy_name = app.conf.config['DEFAULT_QUEUE'].values()[0]
    worker_cnt = 1

    def test_worker_pool_grow(self):
        planner = PlannerPool.get_or_create_planner(self.app, self.queue, self.strategy_name)
        planner.loop_interval = 0.1
        planner.strategy.apply = PickableMock(return_value=self.worker_cnt)
        start_daemon_thread(self.hub.start)
        if wait_condition_till_timeout(self.hub.is_alive, 5, False):
            CouldNotStartException('hub')
        start_daemon_thread(self.guard.start)
        if wait_condition_till_timeout(self.guard.is_alive, 5, False):
            raise CouldNotStartException('guard')
        self.guard.pool.get_or_create_pool_name = PickableMock(return_value=self.queue)
        time.sleep(5)
        self.guard.pool.grow.assert_called_with(self.queue, self.worker_cnt)

    def tearDown(self):
        if self.hub.alive:
            self.hub.stop()
        if self.guard.alive:
            self.guard.stop()
