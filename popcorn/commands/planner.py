import sys
from popcorn.commands import BaseCommand
from popcorn.rpc.pyro import PyroClient
from celery.bin.base import Option
from popcorn.apps.planner import RegisterPlanner


class PlannerCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, command=None):
        self.before_init()
        options = self.generate_options(prog_name, argv, command)
        RegisterPlanner(self.app, **options).start()

    def get_options(self):
        return (
            Option('-Q', '--queue'),
            Option('-S', '--strategy', default='simple'),
            Option('-l', '--loglevel'),
        )
