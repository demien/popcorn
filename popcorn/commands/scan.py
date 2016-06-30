import sys
from popcorn.commands import BaseCommand
from celery.bin.base import Option
from popcorn.apps.scan import Scan, ScanTarget


class ScanCommand(BaseCommand):

    def run_from_argv(self, prog_name, argv=None, command=None):
        command = sys.argv[0] if command is None else command
        argv = sys.argv[1:] if argv is None else argv
        options, args = self.prepare_args(*self.parse_options(prog_name, argv, command))

        scan = Scan(self.app, target=options['target'])
        scan.start()

    def get_options(self):
        return (
            Option('-T', '--target', choices=[ScanTarget.MACHINE, ScanTarget.PLANNER]),
        )
