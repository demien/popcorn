import threading
import Pyro4
import socket
import time

def ip():
    host = socket.gethostname()
    return socket.gethostbyname(host)


def foo():
    daemon = Pyro4.Daemon(host=ip(), port=5555)

    def _start():
        print '*' * 10, 'start pyro server'
        daemon.requestLoop()
        print '*' * 10, 'exit loop'

    thread = threading.Thread(target=daemon.requestLoop)
    thread.daemon = True
    thread.start()
    time.sleep(5)
    daemon.shutdown()
    time.sleep(5)

# foo()

def sleep():
    time.sleep(5)

def td():
    thread = threading.Thread(target=sleep)
    thread.daemon = True
    thread.start()
    import ipdb; ipdb.set_trace()

td()
