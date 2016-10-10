import logging
import unittest
from time import sleep
from mock import MagicMock
from popcorn.apps.hub import Hub
from popcorn.apps.hub.state import DEMAND
from popcorn.apps.hub.order.instruction import InstructionType
from popcorn.apps.planner import PlannerPool
from popcorn.tests.mock_tool import App, Config
from popcorn.utils import start_daemon_thread, wait_condition_till_timeout


logging.basicConfig(level=logging.CRITICAL)


class TestHub(unittest.TestCase):
    app = App()
    hub = Hub(app)
    hub.LOOP_INTERVAL = 0.1

    def setUp(self):
        pass

    def test_start_stop(self):
        self.hub.analyze_demand = MagicMock()
        self.hub.send_order_to_guard = MagicMock()
        self.hub.load_balancing = MagicMock()
        self.assertFalse(self.hub.alive)
        self.assertFalse(self.hub.rpc_server.alive)
        start_daemon_thread(self.hub.start)
        wait_condition_till_timeout(self.hub.is_alive, 5, False)
        if wait_condition_till_timeout(self.hub.is_alive, 5, False):
            raise 'could not start hub'
        self.assertTrue(self.hub.alive)
        self.assertTrue(self.hub.rpc_server.alive)
        self.hub.analyze_demand.assert_called()
        self.hub.send_order_to_guard.assert_called()
        self.hub.load_balancing.assert_not_called()
        self.hub.stop()
        self.assertFalse(self.hub.alive)
        self.assertFalse(self.hub.rpc_server.alive)

    def test_report_demand(self):
        Hub.report_demand(InstructionType.WORKER, 'q1', 1)
        self.assertEqual(DEMAND.keys(), ['q1'])
        self.assertEqual(DEMAND.values(), [1])
        Hub.report_demand(InstructionType.WORKER, 'q1', 2)
        self.assertEqual(DEMAND.values(), [1 + 2])

    def tearDown(self):
        if self.hub.alive:
            self.hub.stop()
