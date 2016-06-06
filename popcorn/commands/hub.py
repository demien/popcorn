from popcorn.commands import BaseCommand


class HubCommand(BaseCommand):

    def run(self, **kwargs):
        print 'hello popcorn, hub'
