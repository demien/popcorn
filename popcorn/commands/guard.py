from popcorn.commands import BaseCommand


class GuardCommand(BaseCommand):

    def run(self, **kwargs):
        print 'hello popcorn, guard'
