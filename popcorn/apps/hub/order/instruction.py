import abc


class Instruction(object):

    __metaclass__ = abc.ABCMeta

    def foo(self, **kwargs):
        raise NotImplementedError()


class WorkerInstruction(Instruction):

    def __init__(self, instruction=None):
        if instruction:
            self._parse(instruction)

    def _parse(self, instruction):
        """
        :param instruction. Example: 'abc:+1' means queue abc add one worker. 'abc:=1 means queue abc have one worker'
        """
