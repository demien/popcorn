import unittest
from mock import MagicMock
from popcorn.apps.guard.machine import CPU, Memory, Hardware, Machine, Camera, psutil


class Foo(object):
    pass


class TestComponent(unittest.TestCase):

    def test_cpu_name(self):
        self.assertEqual(CPU().name, 'cpu')

    def test_cpu_healthy(self):
        cpu_value = Foo()
        setattr(cpu_value, 'idle', CPU.IDLE_THRESHOLD)
        psutil.cpu_times_percent = MagicMock(return_value=cpu_value)
        self.assertFalse(CPU().healthy)
        setattr(cpu_value, 'idle', CPU.IDLE_THRESHOLD + 1)
        self.assertTrue(CPU().healthy)

    def test_memory_name(self):
        self.assertEqual(Memory().name, 'memory')

    def test_memory_healthy(self):
        memory_value = Foo()
        setattr(memory_value, 'available', Memory.AVAILABLE_USAGE_THRESHOLD)
        setattr(memory_value, 'percent', 100 - Memory.AVAILABLE_PERCENTAGE_THRESHOLD)
        psutil.virtual_memory = MagicMock(return_value=memory_value)
        self.assertFalse(Memory().healthy)
        setattr(memory_value, 'available', Memory.AVAILABLE_USAGE_THRESHOLD + 1)
        self.assertTrue(Memory().healthy)
        setattr(memory_value, 'available', Memory.AVAILABLE_USAGE_THRESHOLD)
        setattr(memory_value, 'percent', 100 - Memory.AVAILABLE_PERCENTAGE_THRESHOLD -1)
        self.assertTrue(Memory().healthy)


class TestHardware(unittest.TestCase):

    def test_components(self):
        self.assertEqual(len(Hardware.COMPONENTS), len(['cpu', 'memory']))

    def test_healthy(self):
        cpu_value = Foo()
        memory_value = Foo()
        setattr(cpu_value, 'idle', CPU.IDLE_THRESHOLD + 1)
        setattr(memory_value, 'available', Memory.AVAILABLE_USAGE_THRESHOLD + 1)
        setattr(memory_value, 'percent', 100 - Memory.AVAILABLE_PERCENTAGE_THRESHOLD - 1)
        psutil.cpu_times_percent = MagicMock(return_value=cpu_value)
        psutil.virtual_memory = MagicMock(return_value=memory_value)
        self.assertTrue(Hardware().healthy)
        setattr(cpu_value, 'idle', CPU.IDLE_THRESHOLD)
        self.assertFalse(Hardware().healthy)
        setattr(cpu_value, 'idle', CPU.IDLE_THRESHOLD + 1)
        setattr(memory_value, 'available', Memory.AVAILABLE_USAGE_THRESHOLD)
        setattr(memory_value, 'percent', 100 - Memory.AVAILABLE_PERCENTAGE_THRESHOLD)
        self.assertFalse(Hardware().healthy)


class TestMachine(unittest.TestCase):

    def test_snap_shot(self):
        Machine.SNAPSHOT_SIZE = 1
        machine = Machine()
        machine.snapshot()
        self.assertEqual(len(machine.snapshots), 1)
        machine.snapshot()
        self.assertEqual(len(machine.snapshots), 1)

    def test_healthy(self):
        cpu_value = Foo()
        setattr(cpu_value, 'idle', CPU.IDLE_THRESHOLD)
        self.assertFalse(Machine().healthy)
        self.assertTrue(Machine(healthy_mock=True).healthy)


class TestCamera(unittest.TestCase):

    def test_snap_shot(self):
        snapshot = Camera(Machine()).snapshot()
        self.assertIn('time', snapshot)
        self.assertIn('healthy', snapshot)
        self.assertIn('hardware', snapshot)
