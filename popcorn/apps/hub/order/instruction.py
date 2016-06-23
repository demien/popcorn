import abc
import re
from celery.utils.imports import instantiate



class InstructionType(object):
    WORKER = 'worker'


instruction_mapping = {
    InstructionType.WORKER: 'popcorn.apps.hub.order.instruction.WorkerInstruction',
}


class Instruction(object):
    __metaclass__ = abc.ABCMeta
    SEPERATOR = ':'

    @staticmethod
    def create(type, instruction):
        return instantiate(instruction_mapping[type], instruction)

    def parse(self, instruction):
        raise NotImplementedError()


class Operator(object):
    TO = '='
    INC = '+'
    DEC = '-'
    ALL = [TO, INC, DEC]


class WorkerInstruction(Instruction):

    def __init__(self, instruction):
        self.queue = None
        self.worker_cnt = None
        self.operator = None
        if instruction:
            self.queue, self.operator, self.worker_cnt = self.parse(instruction)
        self.opcodes = (self.queue, self.operator, self.worker_cnt)

    def parse(self, instruction):
        """
        :param instruction. Example: 'abc:+1' means queue abc add one worker. 'abc:=1 means queue abc have one worker'
        """
        pattern = r'(.*)%s(%s)(\d*)' % (self.SEPERATOR, '|'.join(map(re.escape, Operator.ALL)))
        return re.findall(pattern, instruction)[0]
