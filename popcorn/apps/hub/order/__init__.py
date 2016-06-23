from instruction import WorkerInstruction


class Order(object):

    def __init__(self):
        self.instructions = list

    def add_instruction(self, type, instruction):
        new_instruction = Instruction.create(type, instruction)
        self.instructions.append(new_instruction)

