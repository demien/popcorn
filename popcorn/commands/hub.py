from popcorn.commands import BaseCommand
from popcorn.apps.hub import Hub


class HubCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, **_kwargs):
        self.change_default_setting()
        hub = Hub(self.app)
        hub.start()
