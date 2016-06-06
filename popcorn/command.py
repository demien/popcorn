from celery.bin.base import Command


class GuardCommand(Command):
    ENV_VAR_PREFIX = 'POPCORN_'

    def run(self, **kwargs):
        print 'hello popcorn, guard'
