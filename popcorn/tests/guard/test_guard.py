import unittest
import logging
from mock import MagicMock
from popcorn.apps.guard import Guard
from popcorn.tests.mock_tool import App, Config
from popcorn.utils import start_daemon_thread, wait_condition_till_timeout


logging.basicConfig(level=logging.CRITICAL)


class TestGuard(unittest.TestCase):

    app = App()
    guard = Guard(app)
    guard.LOOP_INTERVAL = 0.1

    def test_start_stop(self):
        self.guard._register_to_hub = MagicMock()
        self.guard.heartbeat = MagicMock()
        start_daemon_thread(self.guard.start)
        wait_condition_till_timeout(self.guard.is_alive, 5, False)
        self.assertTrue(self.guard.alive)
        self.guard.heartbeat.assert_called()
        self.guard.stop()
        self.assertFalse(self.guard.alive)

    def tearDown(self):
        if self.guard.alive:
            self.guard.stop()
