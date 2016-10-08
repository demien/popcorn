import re
from operators import Operator
from popcorn.utils import call_callable


class InstructionType(object):
    WORKER = 'worker'


instruction_mapping = {
    InstructionType.WORKER: 'popcorn.apps.hub.order.instruction.WorkerInstruction',
}


class Instruction(object):

    @staticmethod
    def create(cmd, type):
        return call_callable(instruction_mapping[type], cmd)

    @staticmethod
    def load(cmd):
        raise NotImplementedError()

    @staticmethod
    def dump(*args, **kwargs):
        raise NotImplementedError()


class WorkerInstruction(Instruction):

    SEPERATOR = ':'
    TEMPLATE = '%s:%s%s'

    def __init__(self, cmd):
        self.cmd = cmd
        self.queue, self.operator, self.worker_cnt = WorkerInstruction.load(cmd)

    @staticmethod
    def dump(queue, worker_cnt):
        if worker_cnt > 0:
            operator = Operator.INC
        else:
            operator = Operator.DEC
        return WorkerInstruction.TEMPLATE % (queue, operator.slug, str(abs(worker_cnt)))

    @staticmethod
    def load(cmd):
        """
        :param cmd. Example: 'abc:+1' means queue abc add one worker. 'abc:=1 means queue abc have one worker'
        """
        try:
            pattern = r'(.*)%s(%s)(\d*)' % (WorkerInstruction.SEPERATOR, '|'.join(map(lambda x: re.escape(x.slug), Operator.ALL)))
            queue, operator_slug, worker_cnt = re.findall(pattern, cmd)[0]
            operator = Operator.get(operator_slug)
            worker_cnt = int(worker_cnt)
        except:
            raise 'cmd is not valid: %s' % cmd
        return queue, operator, worker_cnt
