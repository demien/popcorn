import ctypes
import threading
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
