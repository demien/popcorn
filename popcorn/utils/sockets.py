import socket


def hostname():
    return socket.gethostname()


def ip():
    return socket.gethostbyname(hostname())
