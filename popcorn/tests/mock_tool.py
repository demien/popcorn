from mock import MagicMock

default_queues = {
    'q1': 'simple',
    'q2': 'simple',
}


class Foo(object):
    pass


class Config(object):

    find_value_for_key = MagicMock()
    get = MagicMock()

    def __init__(self, config):
        self.config = config
    
    def __getitem__(self, key):
        return self.config.get(key, '')


class App(object):
    log = Foo()

    conf = Config({
        'DEFAULT_QUEUE': default_queues,
        'BROKER_URL': '1',
    })

    def __init__(self):
        self.log.setup = MagicMock()
