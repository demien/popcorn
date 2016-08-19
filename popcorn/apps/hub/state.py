from collections import defaultdict

from popcorn.apps.hub.order import Order
from popcorn.apps.hub.order.instruction import WorkerInstruction, InstructionType
from popcorn.utils.log import get_log_obj

debug, info, warn, error, critical = get_log_obj(__name__)

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


#: The source of plan is demand. Load balance module will split the demand in to plan by machine
#: Eg: {queue: {machine: 10}, }
PLAN = defaultdict(lambda: defaultdict(int))


def add_plan(queue, machine, worker_cnt):
    global PLAN
    PLAN[queue][machine] += worker_cnt


def pop_order(machine_id):
    order = Order()
    for queue, _plan in PLAN.iteritems():
        if not _plan[machine_id]:
            continue
        worker_cnt = PLAN[queue].pop(machine_id)
        instruction_cmd = WorkerInstruction.generate_instruction_cmd(queue, worker_cnt)
        order.add_instruction(InstructionType.WORKER, instruction_cmd)
    return None if order.empty else order


#: All the machine register to hub. It's a list.
MACHINES = defaultdict(lambda: None)


def update_machine(machine):
    MACHINES[machine.id] = machine
