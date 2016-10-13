from mock import MagicMock
from popcorn.utils import ip

default_queues = {
    'q1': 'simple',
    'q2': 'simple',
}


class Foo(object):
    pass


class Config(object):

    find_value_for_key = MagicMock()

    def __init__(self, config):
        self.config = config
    
    def __getitem__(self, key):
        return self.config.get(key, '')

    def get(self, key, default=None):
        return self.config.get(key, default)


class App(object):
    log = Foo()

    conf = Config({
        'DEFAULT_QUEUE': default_queues,
        'BROKER_URL': '127.0.0.1',
        'HUB_IP': ip(),
        'HEALTHY_MOCK': True,
    })

    def __init__(self):
        self.log.setup = MagicMock()


class Pool(object):
    pass
