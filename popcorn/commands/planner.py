import sys
from popcorn.commands import BaseCommand
from popcorn.rpc.pyro import PyroClient
from celery.bin.base import Option


class PlannerCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, command=None):
        command = sys.argv[0] if command is None else command
        argv = sys.argv[1:] if argv is None else argv
        import ipdb; ipdb.set_trace()
        # parse options before detaching so errors can be handled.
        options, args = self.prepare_args(
            *self.parse_options(prog_name, argv, command))
        client = PyroClient()
        client.start('popcorn.apps.planner:Planner', queue=options['queue'], strategy='simple')

    def get_options(self):
        return tuple(Option('-Q', '--queue'), Option('-S', 'strategy', default='simple'))
