from celery.bin.base import Option
from popcorn.commands import BaseCommand
from popcorn.apps.hub import Hub


class HubCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, command=None, **_kwargs):
        self.change_default_setting()
        command = sys.argv[0] if command is None else command
        argv = sys.argv[1:] if argv is None else argv
        options, args = self.prepare_args(*self.parse_options(prog_name, argv, command))
        options.pop('app')
        # import ipdb; ipdb.set_trace()
        hub = Hub(self.app, **options)
        hub.start()

    def get_options(self):
        return (
            Option('-l', '--loglevel'),
        )
