from popcorn.commands import BaseCommand
from popcorn.apps.hub import Hub


class HubCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, **_kwargs):
        hub = Hub(self.app)
        hub.start()


    def handle_argv(self, prog_name, argv=None):
        return self.run_from_argv(prog_name, argv)
