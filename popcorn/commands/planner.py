from popcorn.commands import BaseCommand


class PlannerCommand(BaseCommand):

    def run(self, **kwargs):
        print 'hello popcorn, planner'
