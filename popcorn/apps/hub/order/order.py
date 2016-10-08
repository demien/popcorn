from instruction import Instruction, InstructionType


class Order(object):

    def __init__(self):
        self.instructions = []

    def add_instruction(self, instruction, type=InstructionType.WORKER):
        self.instructions.append(Instruction.create(instruction, type))

    @property
    def empty(self):
        return False if self.instructions else True
