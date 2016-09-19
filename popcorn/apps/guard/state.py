from collections import defaultdict
from popcorn.apps.hub.order import Order


#: Orders need to be follow.
#: Hub will update orders
ORDERS = []


def add_order(order):
    ORDERS.append(order)
