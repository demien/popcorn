from collections import defaultdict
from popcorn.apps.hub.order import Order
from popcorn.apps.hub.order.instruction import WorkerInstruction, InstructionType
from popcorn.utils.log import get_log_obj


debug, info, warn, error, critical = get_log_obj(__name__)


def reset():
    reset_demand()
    reset_plan()
    reset_machine()
    reset_planner()

#: Demand update by planner.
#: Planner monitoring the queue and calculate the demond
#: Eg: {queue: 123, }
DEMAND = defaultdict(int)


def add_demand(queue, worker_cnt):
    DEMAND[queue] = worker_cnt


def remove_demand(queue):
    DEMAND.pop(queue)


def get_worker_cnt(queue):
    return DEMAND[queue]


def reset_demand():
    DEMAND.clear()


#: The source of plan is demand. Load balance module will split the demand in to plan by machine
#: Eg: {queue: {machine: 10}, }
PLAN = defaultdict(lambda: defaultdict(int))


def add_plan(queue, machine, worker_cnt):
    PLAN[queue][machine] += worker_cnt


def pop_order(machine_id):
    order = Order()
    for queue, _plan in PLAN.iteritems():
        if not _plan[machine_id]:
            continue
        worker_cnt = PLAN[queue].pop(machine_id)
        instruction_cmd = WorkerInstruction.dump(queue, worker_cnt)
        order.add_instruction(instruction_cmd)
    return None if order.empty else order


def reset_plan():
    PLAN.clear()


#: All the machine register to hub. It's a list.
MACHINES = defaultdict(lambda: None)


def update_machine(machine):
    MACHINES[machine.id] = machine


def reset_machine():
    MACHINES.clear()


#: All the planner. It's a dict. Key is queue name.
PLANNERS = defaultdict(lambda: None)


def reset_planner():
    PLANNERS.clear()

