import unittest
import logging
from mock import MagicMock
from popcorn.apps.guard import Guard
from popcorn.apps.hub import Hub
from popcorn.tests.mock_tool import App, Config
from popcorn.utils import start_daemon_thread, wait_condition_till_timeout
from popcorn.apps.hub import MACHINES


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
            raise 'Could not start guard'
        self.assertTrue(self.guard.alive)
        self.guard.heartbeat.assert_called()
        self.guard.stop()
        self.assertFalse(self.guard.alive)

    def test_heart_beat(self):
        start_daemon_thread(self.hub.start)
        if wait_condition_till_timeout(self.hub.is_alive, 5, False):
            raise 'could not start hub'
        self.assertEqual(len(MACHINES), 0)
        start_daemon_thread(self.guard.start)
        if wait_condition_till_timeout(self.guard.is_alive, 5, False):
            raise 'Could not start guard'
        self.assertEqual(len(MACHINES), 1)

    def tearDown(self):
        if self.hub.alive:
            self.hub.stop()
        if self.guard.alive:
            self.guard.stop()
