from popcorn.command.guard import GuardCommand
from popcorn.command.hub import HubCommand
from popcorn.command.planner import PlannerCommand


def guard():
    try:
        guard = GuardCommand()
        guard.execute_from_commandline()
    except:
        raise

def hub():
    try:
        import ipdb; ipdb.set_trace()
        hub = HubCommand()
        hub.execute_from_commandline()
    except:
        raise


def planner():
    try:
        planner = PlannerCommand()
        planner.execute_from_commandline()
    except:
        raise

def main():
    print '%' * 10, 'in main'

if __name__ == '__main__':  # pragma: no cover
    main()
