import logging
import unittest
from mock import MagicMock
from popcorn.apps.hub import Hub
from popcorn.apps.hub.state import DEMAND
from popcorn.apps.hub.order.instruction import InstructionType
from popcorn.apps.planner import PlannerPool
from popcorn.utils import start_daemon_thread, wait_condition_till_timeout


logging.basicConfig(level=logging.CRITICAL)


class Foo(object):
    pass


class Config(object):

    def __init__(self, config):
        self.config = config
    
    def __getitem__(self, key):
        return self.config.get(key, '')

    def find_value_for_key(self, key):
        return ''


class App(object):
    log = Foo()

    def __init__(self):
        self.log.setup = MagicMock()


class TestHub(unittest.TestCase):
    app = App()
    default_queues = {
        'q1': 'simple',
        'q2': 'simple',
    }
    app.conf = Config({
        'DEFAULT_QUEUE': default_queues,
        'BROKER_URL': '1',
    })
    app.conf.find_value_for_key = MagicMock()
    app.conf.get = MagicMock()
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
