from popcorn.commands import BaseCommand
from popcorn.apps.guard import Guard


class GuardCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, **_kwargs):
        Guard(self.app).start()
