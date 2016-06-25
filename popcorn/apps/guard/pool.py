from multiprocessing import Process
from celery import concurrency
from celery.utils import host_format, default_nodename, node_format
from collections import defaultdict


class Pool(object):

    MIN = 1
    MAX = 10

    def __init__(self, celery_app):
        self.app = celery_app
        self.pool_map = defaultdict(lambda: None)

    def get_or_create_pool_name(self, queue):
        if not self.pool_map.get(queue):
            self.pool_map[queue] = self.create_pool(queue)
        return self.pool_map[queue]['name']

    def create_pool(self, queue, pool_cls=None, loglevel=None, logfile=None, pidfile=None, state_db=None):
        kwargs = {
            'autoscale': '1,1',
            'queues': queue,
        }
        pool_cls = concurrency.get_implementation(pool_cls) or self.app.conf.CELERYD_POOL
        hostname = '%s:%s' % (self.hostname, queue)
        pool = self.app.Worker(
            hostname=hostname,
            pool_cls=pool_cls,
            loglevel=loglevel,
            logfile=logfile,
            pidfile=node_format(pidfile, hostname),
            state_db=node_format(state_db, hostname),
            **kwargs
        )
        process = Process(target=self.start_pool, args=(pool, ))
        process.daemon = True
        process.start()
        return {'name': hostname, 'pool': pool}

    def start_pool(self, pool):
        pool.start()

    def shrink(self, pool_name, cnt):
        max_cnt = max(self.MIN, cnt)
        self.app.control.autoscale(max=max_cnt, min=self.MIN, destination=[pool_name])

    def grow(self, pool_name, cnt):
        max_cnt = min(self.MAX, cnt)
        self.app.control.autoscale(max=min(self.MAX, cnt), min=self.MIN, destination=[pool_name])

    @property
    def hostname(self):
        return host_format(default_nodename(None))
