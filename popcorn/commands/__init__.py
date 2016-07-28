from celery.bin.base import Command, Option


class BaseCommand(Command):
    ENV_VAR_PREFIX = 'POPCORN_'

    def before_init(self):
        self.change_default_setting()

    def change_default_setting(self):
        self.app.conf['CELERYD_AUTOSCALER'] = 'popcorn.apps.guard.autoshrink:Autoshrink'

    def generate_options(self, prog_name, argv, command):
        command = sys.argv[0] if command is None else command
        argv = sys.argv[1:] if argv is None else argv
        options, args = self.prepare_args(*self.parse_options(prog_name, argv, command))
        options.pop('app')
        return options

    def get_options(self):
        return (
            Option('-l', '--loglevel'),
        )
