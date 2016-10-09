import unittest
from popcorn.apps.hub.order.order import Order
from popcorn.apps.hub.order.Instruction import WorkerInstruction
from popcorn.apps.exceptions import InstructionCMDException


class TestOrder(unittest.TestCase):
    cmd = 'abc:+1'

    def test_empty(self):
        self.assertTrue(Order().empty)

    def test_add_instruction(self):
        order = Order()
        order.add_instruction(self.cmd)
        self.assertEqual(order.length , 1)
        self.assertTrue(order.instructions[0] == WorkerInstruction(self.cmd))
