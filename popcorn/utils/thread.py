import ctypes


def get_pid():
    SYS_gettid = 186
    libc = ctypes.cdll.LoadLibrary('libc.so.6')
    return libc.syscall(SYS_gettid)
