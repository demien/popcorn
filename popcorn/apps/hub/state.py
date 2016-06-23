from collections import defaultdict
from popcorn.apps.hub.order import Order
from popcorn.apps.hub.order.instruction import WorkerInstruction, InstructionType


#: Demand update by planner.
#: Planner monitoring the queue and calculate the demond
#: Eg: {queue: 123, }
DEMAND = defaultdict(int)


def add_demand(queue, worker_cnt):
    global DEMAND
    DEMAND[queue] = worker_cnt


def remove_demand(queue):
    global DEMAND
    DEMAND.POP(queue)


#: The source of plan is demand. Load balance module will split the demand in to plan by machine
#: Eg: {queue: {machine: 10}, }
PLAN = defaultdict(lambda: defaultdict(int))


def add_plan(queue, machine, worker_cnt):
    global PLAN
    PLAN[queue][machine] += worker_cnt


def pop_order(machine_id):
    global PLAN
    order = Order()
    for queue, _plan in PLAN.iteritems():
        if not _plan[machine_id]:
            continue
        worker_cnt = PLAN[queue].pop(machine_id)
        instruction_cmd = WorkerInstruction.generate_instruction_cmd(queue, worker_cnt)
        order.add_instruction(InstructionType.WORKER, instruction_cmd)
    return order


MACHINES = defaultdict(lambda: None)

def update_machine(machine):
    global MACHINES
    MACHINES[machine.id] = machine
