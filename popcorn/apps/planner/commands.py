from popcorn.apps.planner import PlannerPool
from popcorn.utils import get_log_obj


debug, info, warn, error, critical = get_log_obj(__name__)


def start_planner(app, queue, strategy_name):
    info('[Planner] - [Register] - Queue: %s, Strategy: %s' % (queue, strategy_name))
    planner = PlannerPool.get_or_create_planner(app, queue, strategy_name)
    if planner.alive:
        planner.load_strategy(strategy_name)
    else:
        planner.start()

def stop_planner(queue):
    info('[Planner] - [Stop] - Queue: %s' % queue)
    planner = PlannerPool.get(queue)
    if planner is None:
        debug('[Planner] - [Not Found] - Queue: %s' % queue)
    else:
        planner.stop()
