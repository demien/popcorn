from popcorn.rpc.pyro import PyroClient, GUARD_PORT
from popcorn.apps.hub.order import Order
from popcorn.utils import ip

c = PyroClient(ip(), GUARD_PORT)
o = Order()
o.add_instruction('demien:-5')
c.call('popcorn.apps.guard.commands.receive_order', o)
