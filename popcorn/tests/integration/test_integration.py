import unittest
import time
import logging
from mock import MagicMock
from popcorn.apps import planner as _planner
from popcorn.apps.exceptions import CouldNotStartException
from popcorn.apps.hub import Hub, state
from popcorn.apps.guard import Guard
from popcorn.apps.planner import PlannerPool, taste_soup
from popcorn.apps.utils import broker_util
from popcorn.tests.mock_tool import App, Pool, PickableMock
from popcorn.utils import start_daemon_thread, wait_condition_till_timeout
from popcorn.apps.guard import Guard, commands


logging.basicConfig(level=logging.CRITICAL)


class TestIntegration(unittest.TestCase):
    app = App()
    labels = ['a']
    guard = Guard(app, **{'labels': ','.join(labels)})
    guard.LOOP_INTERVAL = 0.1
    guard.pool = Pool()
    guard.pool.grow = PickableMock()
    hub = Hub(app)
    hub.LOOP_INTERVAL = 0.1
    queue = app.conf.config['DEFAULT_QUEUE'].keys()[0]
    strategy_name = app.conf.config['DEFAULT_QUEUE'][queue]['strategy']
    worker_cnt = 1

    def setUp(self):
        _planner.taste_soup = PickableMock(return_value=0)

    def test_worker_pool_grow(self):
        planner = PlannerPool.get_or_create_planner(self.queue, self.app, self.strategy_name, self.labels)
        planner.loop_interval = 0.1
        planner.strategy.apply = PickableMock(return_value=self.worker_cnt)
        start_daemon_thread(self.hub.start)
        if wait_condition_till_timeout(self.hub.is_alive, 5, False):
            CouldNotStartException('hub')
        start_daemon_thread(self.guard.start)
        if wait_condition_till_timeout(self.guard.is_alive, 5, False):
            raise CouldNotStartException('guard')
        self.guard.pool.get_or_create_pool_name = PickableMock(return_value=self.queue)
        time.sleep(1)
        self.guard.pool.grow.assert_called_with(self.queue, self.worker_cnt)
        planner.stop()

    def test_guard_without_label_could_not_get_order(self):
        mylabels = ['z']
        planner = PlannerPool.get_or_create_planner(self.queue, self.app, self.strategy_name, mylabels)
        planner.loop_interval = 0.1
        planner.strategy.apply = PickableMock(return_value=self.worker_cnt)
        start_daemon_thread(self.hub.start)
        if wait_condition_till_timeout(self.hub.is_alive, 5, False):
            CouldNotStartException('hub')
        start_daemon_thread(self.guard.start)
        if wait_condition_till_timeout(self.guard.is_alive, 5, False):
            raise CouldNotStartException('guard')
        self.guard.pool.get_or_create_pool_name = PickableMock(return_value=self.queue)
        time.sleep(1)
        self.guard.pool.grow.assert_not_called()
        planner.stop()

    def tearDown(self):
        PlannerPool.reset()
        state.reset()
        if self.hub.alive:
            self.hub.stop()
        if self.guard.alive:
            self.guard.stop()
