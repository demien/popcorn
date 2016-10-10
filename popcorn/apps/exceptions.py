class PlannerException(Exception):
    pass


class OperatorApplyException(Exception):
    def __init__(self, message=''):
        message = message or 'Invalid operator apply params.'
        super(OperatorApplyException, self).__init__(message)


class InstructionCMDException(Exception):
    def __init__(self, cmd):
        message = 'Invalid cmd: %s when build instruction' % cmd
        super(InstructionCMDException, self).__init__(message)


class CouldNotStopException(Exception):
    def __init__(self, target):
        message = 'Could not stop %s.' % target
        super(CouldNotStopException, self).__init__(message)
