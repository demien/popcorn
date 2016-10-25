# -*- coding: utf-8 -*-
"""
    celery.worker.autoscale
    ~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the internal thread responsible
    for growing and shrinking the pool according to the
    current autoscale settings.

    The autoscale thread is only enabled if :option:`--autoscale`
    has been enabled on the command-line.

"""
from __future__ import absolute_import

import os
import threading

from time import sleep

from kombu.async.semaphore import DummyLock

from celery import bootsteps
from celery.five import monotonic
from celery.utils.log import get_logger
from celery.utils.threads import bgThread

from celery.worker import state
from celery.worker.components import Pool

__all__ = ['Autoshrink']

logger = get_logger(__name__)
debug, info, error = logger.debug, logger.info, logger.error

AUTOSCALE_KEEPALIVE = float(os.environ.get('AUTOSCALE_KEEPALIVE', 30))


class Autoshrink(bgThread):

    def __init__(self, pool, max_concurrency,
                 min_concurrency=0, worker=None,
                 keepalive=AUTOSCALE_KEEPALIVE, mutex=None):
        super(Autoshrink, self).__init__()
        self.pool = pool
        self.mutex = mutex or threading.Lock()
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self.keepalive = keepalive
        self._last_action = None
        self.worker = worker

        assert self.keepalive, 'cannot scale down too fast.'

    def body(self):
        with self.mutex:
            self.maybe_scale()
        sleep(1.0)

    def _maybe_scale(self, req=None):
        procs = self.processes
        cur = self.qty
        if cur < procs:
            cnt = (procs - cur) - self.min_concurrency
            self.scale_down(max(cnt, 0))
            return True

    def maybe_scale(self, req=None):
        if self._maybe_scale(req):
            self.pool.maintain_pool()

    def update(self, max=None, min=None):
        with self.mutex:
            if max is not None:
                if max < self.max_concurrency:
                    self._shrink(self.processes - max)
                self.max_concurrency = max
            if min is not None:
                if min > self.min_concurrency:
                    self._grow(min - self.min_concurrency)
                self.min_concurrency = min
            return self.max_concurrency, self.min_concurrency

    def force_scale_up(self, n):
        with self.mutex:
            new = self.processes + n
            if new > self.max_concurrency:
                self.max_concurrency = new
            self.min_concurrency += 1
            self._grow(n)

    def force_scale_down(self, n):
        with self.mutex:
            new = self.processes - n
            if new < self.min_concurrency:
                self.min_concurrency = max(new, 0)
            self._shrink(min(n, self.processes))

    def scale_up(self, n):
        self._last_action = monotonic()
        return self._grow(n)

    def scale_down(self, n):
        if self._last_action:
            if n and (monotonic() - self._last_action > self.keepalive):
                self._last_action = monotonic()
                return self._shrink(n)
        else:
            self._last_action = monotonic()
            # return self._shrink(n)

    def _grow(self, n):
        info('Scaling up %s processes.', n)
        self.pool.grow(n)
        self.worker.consumer._update_prefetch_count(n)

    def _shrink(self, n):
        info('Scaling down %s processes.', n)
        try:
            self.pool.shrink(n)
        except ValueError:
            debug("Autoscaler won't scale down: all processes busy.")
        except Exception as exc:
            error('Autoscaler: scale_down: %r', exc, exc_info=True)
        self.worker.consumer._update_prefetch_count(-n)

    def info(self):
        return {'max': self.max_concurrency,
                'min': self.min_concurrency,
                'current': self.processes,
                'qty': self.qty}

    @property
    def qty(self):
        return len(state.reserved_requests)

    @property
    def processes(self):
        return self.pool.num_processes
