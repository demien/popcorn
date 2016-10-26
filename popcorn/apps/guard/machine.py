import abc
import psutil
import socket
from collections import defaultdict
from datetime import datetime
from popcorn.utils import ip as _ip, hostname as _hostname, white


class Machine(object):

    SNAPSHOT_SIZE = 10

    def __init__(self, healthy_mock=False, labels=[]):
        self.hardware = Hardware()
        self.camera = Camera(self)
        self.snapshots = []  # lastest n snapshot
        self.id = self.get_id()
        self.healthy_mock = healthy_mock
        self.labels = labels

    def __repr__(self):
        return 'ID: %s. Labels: %s.' % (self.id, ','.join(self.labels))

    @property
    def ip(self):
        return _ip()

    @property
    def hostname(self):
        return _hostname()

    def get_id(self):
        return self.ip

    @property
    def healthy(self):
        if self.healthy_mock:
            return True
        return self.hardware.healthy

    def set_labels(self, labels):
        self.labels = labels

    def snapshot(self, extra_info={}):
        snapshot = self.camera.snapshot(extra_info)
        if len(self.snapshots) >= self.SNAPSHOT_SIZE:
            self.snapshots.pop(0)
        self.snapshots.append(snapshot)
        return snapshot


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

    IDLE_THRESHOLD = 10  # percentage

    @property
    def value(self):
        return psutil.cpu_times_percent()

    @property
    def name(self):
        return 'cpu'

    @property
    def display_name(self):
        return 'CPU'

    @property
    def healthy(self):
        return self.value.idle > self.IDLE_THRESHOLD


class Memory(Component):

    G = 1024 * 1024 * 1024  # gigabyte
    AVAILABLE_USAGE_THRESHOLD = 0.5 * G
    AVAILABLE_PERCENTAGE_THRESHOLD = 30  # percentage

    @property
    def value(self):
        return psutil.virtual_memory()

    @property
    def name(self):
        return 'memory'

    @property
    def display_name(self):
        return 'Memory'

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

    def to_string(self):
        return '\n'.join(['\t%s: %s ' % (component.display_name, component.value) for component in self.COMPONENTS])

    def to_term(self):
        return '\n'.join(['\t%s: %s ' % (white(component.display_name), component.value) for component in self.COMPONENTS])


class Camera(object):
    """camera take snapshot for machine hardware"""
    def __init__(self, machine):
        self.machine = machine

    def snapshot(self, extra_info={}):
        snapshot = {
            'time': datetime.now(),
            'healthy': self.machine.healthy,
            'hardware': self.machine.hardware.to_term(),
        }
        if self.machine.labels:
            snapshot['labels'] = self.machine.labels
        snapshot.update({'extra': extra_info})
        return snapshot
