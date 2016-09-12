import unittest
import threading
import time
from popcorn.rpc.pyro import PyroServer, PyroClient


class PyroBase(unittest.TestCase):

    def setUp(self):

        self.server = self.create_server()
        self.client = self.create_client()
        self.server.start()

    def create_server(self):
        return PyroServer()

    def create_client(self):
        return PyroClient(self.server.ip)

    def tearDown(self):
        self.server.stop()


class TestServer(PyroBase):

    def test_server_start_stop(self):
        self.assertEqual(type(self.server.thread), threading.Thread)
        self.assertTrue(self.server.thread.is_alive())
        self.server.stop()
        self.assertFalse(self.server.thread.is_alive())


class TestClient(PyroBase):

    def test_get_uri(self):
        obj_id = 'obj_id'
        uri = 'PYRO:%s@%s:%s' % (obj_id, self.server.ip, self.server.port)
        self.assertEqual(self.client.get_uri(obj_id, self.server.ip, self.server.port), uri)

    def test_call(self):
        foo = 'foo1'
        call_return = self.client.call('popcorn.tests.rpc.test_pyro.echo', foo)
        self.assertEqual(foo, call_return)

    def test_call_return_with_class(self):
        call_return = self.client.call('popcorn.tests.rpc.test_pyro.echo', pickle_class)
        self.assertEqual(pickle_class, call_return)

    def test_call_return_with_class_instance(self):
        foo = pickle_class()
        call_return = self.client.call('popcorn.tests.rpc.test_pyro.echo', foo)
        self.assertTrue(isinstance(call_return, pickle_class))
        self.assertEqual(str(foo), str(call_return))


class pickle_class(object):
    age = 1
    def name(self):
        return 'app annie'
    def __repr__(self):
        return '%s, %s' % (self.name(), self.age)


def echo(foo):
    return foo


if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    [suite.addTest(loader.loadTestsFromTestCase(_cls)) for _cls in [TestServer, TestClient]]
    unittest.TextTestRunner(verbosity=2).run(suite)
