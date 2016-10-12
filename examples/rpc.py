from popcorn.rpc.pyro import PyroServer, PyroClient
s1 = PyroServer(9000)
s2 = PyroServer(9001)
s1.start()
s2.start()

c1 = PyroClient(s1.ip, 9000)
c2 = PyroClient(s2.ip, 9001)
print c1.call('popcorn.tests.rpc.test_pyro.echo', 'hello')
print c2.call('popcorn.tests.rpc.test_pyro.echo', 'world')
s1.stop()
s2.stop()

def echo(a):
    return a
