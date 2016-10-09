import unittest
from popcorn.apps.hub.order.Instruction import Instruction, WorkerInstruction, Operator, InstructionType
from popcorn.apps.exceptions import InstructionCMDException


class TestInstruction(unittest.TestCase):
    cmd = 'abc:+1'

    def test_load(self):
        instruction = WorkerInstruction(self.cmd)
        self.assertEqual(instruction.queue, 'abc')
        self.assertEqual(instruction.operator, Operator('+'))
        self.assertEqual(instruction.worker_cnt, 1)

    def test_invalid_load(self):
        self.assertRaises(InstructionCMDException, WorkerInstruction.load, 'abc+1')

    def test_dump(self):
        self.assertEqual(WorkerInstruction.dump('abc', 1), self.cmd)
        self.assertEqual(WorkerInstruction.dump('abc', -1), 'abc:-1')

    def test_create_instruction(self):
        instruction = Instruction.create(self.cmd, InstructionType.WORKER)
        self.assertEqual(instruction, WorkerInstruction(self.cmd))
