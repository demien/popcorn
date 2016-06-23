import abc
import re
import operator
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


class BaseOperator(object):

    def __init__(self, slug, apply):
        self.slug = slug
        self.apply = apply


class Operator(object):
    TO = BaseOperator('=', lambda a, b: b)
    INC = BaseOperator('+', operator.add)
    DEC = BaseOperator('-', operator.sub)
    ALL = [TO, INC, DEC]


class WorkerInstruction(Instruction):

    TEMPLATE = '%s:%s%s'

    def __init__(self, instruction):
        self.queue = None
        self.worker_cnt = None
        self.operator = None
        if instruction:
            self.queue, self.operator, self.worker_cnt = self.parse(instruction)
        self.opcodes = (self.queue, self.operator, self.worker_cnt)

    @staticmethod
    def generate_instruction_cmd(queue, worker_cnt):
        if worker_cnt > 0:
            operator = Operator.INC
        else:
            operator = Operator.DEC
        return WorkerInstruction.TEMPLATE % (queue, operator.slug, str(abs(worker_cnt)))

    def parse(self, instruction):
        """
        :param instruction. Example: 'abc:+1' means queue abc add one worker. 'abc:=1 means queue abc have one worker'
        """
        pattern = r'(.*)%s(%s)(\d*)' % (self.SEPERATOR, '|'.join(map(lambda x: re.escape(x.slug), Operator.ALL)))
        return re.findall(pattern, instruction)[0]
