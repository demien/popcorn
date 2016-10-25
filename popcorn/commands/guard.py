from celery.bin.base import Option

from popcorn.commands import BaseCommand
from popcorn.apps.guard import Guard


class GuardCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, command=None, **_kwargs):
        self.before_init()
        options = self.generate_options(prog_name, argv, command)
        Guard(self.app, **options).start()

    def get_options(self):
        return (
            Option('--labels', default=''),
            Option('-l', '--loglevel'),
        )
