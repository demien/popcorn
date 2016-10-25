import time
from celery import concurrency
from celery.utils import host_format, default_nodename, node_format
from collections import defaultdict
from popcorn.apps.exceptions import CouldNotStartException
from popcorn.utils import get_log_obj, start_daemon_thread
from multiprocessing import Process


debug, info, warn, error, critical = get_log_obj(__name__)


class Pool(object):

    MIN = 1
    MAX = 20

    def __init__(self, celery_app):
        self.app = celery_app
        self.pool_map = defaultdict(lambda: defaultdict(lambda: None))

    def get_or_create_pool_name(self, queue):
        if self.pool_map[queue]['pool'] is None:
            self.pool_map[queue] = self.create_pool(queue)
        return self.pool_map[queue]['name']

    @property
    def pinfo(self):
        re = defaultdict(int)
        try:
            for queue, info in self.pool_map.iteritems():
                pool = info.get('pool')
                if pool is not None:
                    re[queue] = pool.pool.num_processes
        except:
            pass
        return {'Queue & Cnt': re}

    def create_pool(self, queue, pool_cls=None, loglevel=None, logfile=None, pidfile=None, state_db=None):
        kwargs = {
            'autoscale': '200,1',
            'queues': queue,
        }
        pool_cls = concurrency.get_implementation(pool_cls) or self.app.conf.CELERYD_POOL
        hostname = '%s::%s' % (self.hostname, queue)
        pool = self.app.Worker(
            hostname=hostname,
            pool_cls=pool_cls,
            loglevel=loglevel,
            logfile=logfile,
            pidfile=node_format(pidfile, hostname),
            state_db=node_format(state_db, hostname),
            without_mingle=False,
            **kwargs
        )
        start_daemon_thread(pool.start)
        if self.check_pool_start(hostname):
            return {'name': hostname, 'pool': pool}
        else:
            raise CouldNotStartException('Work Pool')

    def check_pool_start(self, destination):
        try:
            self.app.control.ping([destination], 5)
            ping_result = self.app.control.ping([destination], 5)
            return ping_result[0][destination] == {u'ok': u'pong'}
        except:
            return False

    def shrink(self, pool_name, cnt):
        self.shrink_by_worker_pool(cnt, destination=[pool_name])

    def grow(self, queue, cnt):
        self.grow_by_worker_pool(queue, cnt)

    def grow_by_app_control(self, queue, cnt):
        pool_name = self.get_or_create_pool_name(queue)
        cnt = min(self.MAX, cnt)
        info('[Pool] - [Grow] - %s, %s' % (pool_name, str(cnt)))
        self.app.control.pool_grow(cnt, destination=[pool_name])

    def shrink_by_app_control(self, queue, cnt):
        pool_name = self.get_or_create_pool_name(queue)
        info('[Pool] - [Shrink] - %s, %s' % (pool_name, str(cnt)))
        self.app.control.pool_shrink(cnt, destination=[pool_name])

    def grow_by_worker_pool(self, queue, cnt):
        cnt = min(self.MAX, cnt)
        self.get_or_create_pool_name(queue)
        info('[Pool] - [Grow] - %s, %s' % (queue, str(cnt)))
        self.pool_map.get(queue)['pool'].pool.grow(cnt)

    def shrink_by_worker_pool(self, queue, cnt):
        self.get_or_create_pool_name(queue)
        info('[Pool] - [Shrink] - %s, %s' % (queue, str(cnt)))
        self.pool_map.get(queue)['pool'].pool.shrink(cnt)

    @property
    def hostname(self):
        return host_format(default_nodename(None))

    def to_string(self):
        re = ''
        for queue, pool in self.pool_map.iteritems():
            re += 'queue, %s' % pool
        return re
