from instruction import Instruction


class Order(object):

    def __init__(self):
        self.instructions = []

    def add_instruction(self, type, instruction):
        new_instruction = Instruction.create(type, instruction)
        self.instructions.append(new_instruction)

