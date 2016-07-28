from popcorn.commands import BaseCommand
from popcorn.apps.hub import Hub


class HubCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, command=None, **_kwargs):
        self.before_init()
        options = self.generate_options(prog_name, argv, command)
        Hub(self.app, **options).start()
