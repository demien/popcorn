import ctypes
import threading
import time
from popcorn.utils.log import get_log_obj


debug, info, warn, error, critical = get_log_obj(__name__)


def get_pid():
    SYS_gettid = 186
    libc = ctypes.cdll.LoadLibrary('libc.so.6')
    return libc.syscall(SYS_gettid)


def start_daemon_thread(target, args=()):
    debug('[BG] Tast New Thread %r', target)
    thread = threading.Thread(target=target, args=args)
    thread.daemon = True
    thread.start()
    return thread


def terminate_thread(thread):
    """Terminates a python thread from another thread.

    :param thread: a threading.Thread instance
    """
    if not thread.isAlive():
        return

    exc = ctypes.py_object(SystemExit)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(thread.ident), exc)
    if res == 0:
        raise ValueError("nonexistent thread id")
    elif res > 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread.ident, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


def wait_condition_till_timeout(condition, seconds, true_condition=True):
    end_time = time.time() + seconds
    if true_condition:
        while condition() and time.time() < end_time:
            continue
        return condition()
    else:
        while not condition() and time.time() < end_time:
            continue
        return not condition()


class TimeOut(object):
    def __init__(self, seconds):
        self.start = time.time()
        self.seconds = seconds

    def check(self):
        return time.time() > self.start + self.seconds

