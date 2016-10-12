import unittest
import logging
from collections import defaultdict
from mock import MagicMock
from popcorn.apps import planner
from popcorn.apps.planner import PlannerPool, Planner
from popcorn.apps.planner.commands import start_planner, stop_planner
from popcorn.apps.planner.strategy.simple import SimpleStrategy
from popcorn.apps.hub.state import get_worker_cnt, reset_demand
from popcorn.utils import wait_condition_till_timeout


logging.basicConfig(level=logging.CRITICAL)


class App(object):
    conf = {'BROKER_URL': 'foo'}


class TestPlanner(unittest.TestCase):
    queue = 'foo'
    strategy_name = 'simple'
    app = App()

    def setUp(self):
        reset_demand()
        planner.taste_soup = MagicMock(return_value=1)
        self.planner = Planner(self.app, self.queue, self.strategy_name)
        self.planner.loop_interval = 0.1

    def test_start_stop(self):
        self.planner.start()
        self.assertTrue(self.planner.alive)
        self.planner.stop()
        self.assertFalse(self.planner.alive)

    def test_force_quite(self):
        self.planner.start()
        self.assertTrue(self.planner.alive)
        self.planner.force_quit()
        wait_condition_till_timeout(self.planner.is_alive, 10)
        self.assertFalse(self.planner.alive)

    def test_loop_report_demand(self):
        self.assertEqual(get_worker_cnt(self.queue), 0)
        self.planner.strategy.apply = MagicMock(return_value=1)
        self.planner.start()
        wait_condition_till_timeout(lambda: True, 1)
        self.planner.strategy.apply.assert_called()
        self.assertTrue(get_worker_cnt(self.queue) > 0)

    def test_loop_raise_exception(self):
        self.assertEqual(get_worker_cnt(self.queue), 0)
        self.planner.strategy.apply = MagicMock(return_value='invalid string value')
        self.planner.start()
        wait_condition_till_timeout(lambda: True, 1)
        self.planner.strategy.apply.assert_called()
        self.assertTrue(get_worker_cnt(self.queue) == 0)

    def test_command(self):
        planner = start_planner(self.app, self.queue, self.strategy_name)
        planner.loop_interval = 0.1
        self.assertTrue(planner.alive)
        _planner = stop_planner(self.queue)
        self.assertTrue(planner == _planner)
        self.assertFalse(planner.alive)

    def test_planner_pool(self):
        planner = PlannerPool.get_or_create_planner(self.app, self.queue, self.strategy_name)
        _planner = PlannerPool.get_or_create_planner(self.app, self.queue, self.strategy_name)
        self.assertEqual(planner, _planner)
        self.assertEqual(PlannerPool.stats().keys(), [self.queue])

    def test_planner_pool_stop(self):
        planner_1 = PlannerPool.get_or_create_planner(self.app, self.queue, self.strategy_name)
        planner_2 = PlannerPool.get_or_create_planner(self.app, 'q2', self.strategy_name)
        PlannerPool.stop()
        self.assertFalse(planner_1.alive)
        self.assertFalse(planner_2.alive)

    def tearDown(self):
        if self.planner.alive:
            self.planner.stop()
