from popcorn.apps.scan import ScanTarget
from popcorn.apps.planner import PlannerPool
from popcorn.utils import get_log_obj
from .state import update_machine as _update_machine, MACHINES, add_demand


debug, info, warn, error, critical = get_log_obj(__name__)


def register_machine(machine):
    info('[HUB] - [Receive] - [Machine Register] : %s' % machine)
    update_machine(machine)


def update_machine(machine):
    debug('[HUB] - [Receive] - [Machine Update] : %s' % machine)
    _update_machine(machine)


def scan(target):
    if target == ScanTarget.MACHINE:
        return dict(MACHINES)
    if target == ScanTarget.PLANNER:
        return PlannerPool.stats()
