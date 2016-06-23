import abc
import psutil
import socket
from collections import defaultdict
from datetime import datetime


class Machine(object):

    SNAPSHOT_SIZE = 10

    _CPU_WINDOW_SIZE = 5

    def __init__(self):
        self.hardware = Hardware()
        self.camera = Camera(self)
        self.snapshots = []  # lastest n snapshot
        self._plan = defaultdict(int)

    @property
    def id(self):
        name = socket.gethostname()
        ip = socket.gethostbyname(name)
        return '%s@%s' % (name, ip)

    @property
    def healthy(self):
        return self.hardware.healthy

    def snapshot(self):
        snapshot = self.camera.snapshot()
        if len(self.snapshots) >= self.SNAPSHOT_SIZE:
            self.snapshots.pop(0)
        self.snapshots.append(snapshot)
        return snapshot

    def get_worker_number(self, queue):
        return self._plan[queue]

    def current_worker_number(self, queue):
        return 1

    def plan(self, *queues):
        return {queue: self.get_worker_number(queue) for queue in queues}

    def add_plan(self, queue, worker_number):
        self._plan[queue] += worker_number

    def clear_plan(self):
        self._plan = defaultdict(int)


class Component(object):
    __metaclass__ = abc.ABCMeta

    @property
    def name(self):
        raise NotImplementedError()
    
    @property
    def value(self):
        raise NotImplementedError()

    @property
    def healthy(self):
        raise NotImplementedError()


class CPU(Component):

    IDLE_THRESHOLD = 30  # percentage

    @property
    def value(self):
        return psutil.cpu_times_percent()

    @property
    def name(self):
        return 'cpu'

    @property
    def healthy(self):
        return self.value.idle > self.IDLE_THRESHOLD


class Memory(Component):

    G = 1024 * 1024 * 1024  # gigabyte
    AVAILABLE_USAGE_THRESHOLD = 2 * G
    AVAILABLE_PERCENTAGE_THRESHOLD = 40  # percentage

    @property
    def value(self):
        return psutil.virtual_memory()

    @property
    def name(self):
        return 'memory'

    @property
    def healthy(self):
        return (self.value.available > self.AVAILABLE_USAGE_THRESHOLD) or \
                (self.value.percent < (100 - self.AVAILABLE_PERCENTAGE_THRESHOLD))


class Hardware(object):

    COMPONENTS = (CPU(), Memory())

    def __init__(self):
        self._assembly()

    def _assembly(self):
        for component in self.COMPONENTS:
            setattr(self, component.name, component)

    @property
    def healthy(self):
        return all([c.healthy for c in self.COMPONENTS])


class Camera(object):
    """camera take snapshot for machine hardware"""
    def __init__(self, machine):
        self.machine = machine

    def snapshot(self):
        snapshot = {'time': datetime.now()}
        for component in self.machine.hardware.COMPONENTS:
            picture[component.name] = component.value
        return snapshot
