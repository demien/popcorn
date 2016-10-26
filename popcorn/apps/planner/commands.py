from popcorn.apps.planner import PlannerPool
from popcorn.utils import get_log_obj


debug, info, warn, error, critical = get_log_obj(__name__)


def start_planner(app, queue, strategy_name, labels):
    planner = PlannerPool.get_or_create_planner(queue, app, strategy_name, labels)
    if planner.alive:
        planner.load(strategy_name, labels)
        info('[Planner] - [Load] - %s' % planner)
    else:
        planner.start()
        info('[Planner] - [Register] - %s' % planner)
    return planner


def stop_planner(queue):
    info('[Planner] - [Stop] - Queue: %s' % queue)
    planner = PlannerPool.pool[queue]
    if planner is None:
        debug('[Planner] - [Not Found] - Queue: %s' % queue)
    else:
        planner.stop()
    return planner
