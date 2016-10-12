import unittest
import logging
import time
from mock import MagicMock
from popcorn.apps.exceptions import CouldNotStartException
from popcorn.apps.guard import Guard, commands
from popcorn.apps.hub import MACHINES, PLAN, reset, Hub
from popcorn.apps.hub.order.instruction import InstructionType
from popcorn.tests.mock_tool import App, Config
from popcorn.utils import start_daemon_thread, wait_condition_till_timeout


logging.basicConfig(level=logging.CRITICAL)


class TestGuard(unittest.TestCase):

    app = App()
    guard = Guard(app)
    guard.LOOP_INTERVAL = 0.1
    hub = Hub(app)
    hub.LOOP_INTERVAL = 0.1

    def test_start_stop(self):
        self.guard._register_to_hub = MagicMock()
        self.guard.heartbeat = MagicMock()
        start_daemon_thread(self.guard.start)
        if wait_condition_till_timeout(self.guard.is_alive, 5, False):
            raise CouldNotStartException('guard')
        self.assertTrue(self.guard.alive)
        self.guard.heartbeat.assert_called()
        self.guard.stop()
        self.assertFalse(self.guard.alive)

    def test_heart_beat(self):
        PLAN = {'q1': {self.guard.machine.id: 1}}
        start_daemon_thread(self.hub.start)
        if wait_condition_till_timeout(self.hub.is_alive, 5, False):
            raise CouldNotStartException('hub')
        start_daemon_thread(self.guard.start)
        if wait_condition_till_timeout(self.guard.is_alive, 5, False):
            raise CouldNotStartException('guard')
        Hub.report_demand(InstructionType.WORKER, 'q1', 2)
        commands.receive_order = MagicMock()
        time.sleep(2)
        commands.receive_order.assert_called()

    def test_register_to_hub(self):
        reset()
        start_daemon_thread(self.hub.start)
        if wait_condition_till_timeout(self.hub.is_alive, 5, False):
            raise CouldNotStartException('hub')
        self.assertEqual(len(MACHINES), 0)
        start_daemon_thread(self.guard.start)
        if wait_condition_till_timeout(self.guard.is_alive, 5, False):
            raise CouldNotStartException('guard')
        self.assertEqual(len(MACHINES), 1)
        self.assertEqual(MACHINES.keys(), [self.guard.machine.id])

    def tearDown(self):
        if self.hub.alive:
            self.hub.stop()
        if self.guard.alive:
            self.guard.stop()
