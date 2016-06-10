from popcorn.commands import BaseCommand
from popcorn.rpc.pyro import PyroClient


class PlannerCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, **_kwargs):
        client = PyroClient()
        client.start('popcorn.apps.planner:Planner')
