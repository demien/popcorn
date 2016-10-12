from instruction import Instruction, InstructionType


class Order(object):

    def __init__(self):
        self.instructions = []

    def __eq__(self, another):
        if len(self.instructions) <> len(another.instructions):
            return False
        for i in range(len(self.instructions)):
            if self.instructions[i] != another.instructions[i]:
                return False
        return True

    def add_instruction(self, cmd, type=InstructionType.WORKER):
        self.instructions.append(Instruction.create(cmd, type))

    @property
    def empty(self):
        return False if self.instructions else True

    @property
    def length(self):
        return len(self.instructions)
