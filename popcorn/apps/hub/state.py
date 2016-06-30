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
    DEMAND.pop(queue)

def get_worker_cnt(queue):
    global DEMAND
    return DEMAND[queue]


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
    return None if order.empty else order


#: All the machine register to hub. It's a list.
MACHINES = defaultdict(lambda: None)

def update_machine(machine):
    global MACHINES
    MACHINES[machine.id] = machine


#: All the planner, including queue and strategy
PLANNERS = defaultdict(str)

def add_planner(queue, strategy):
    global PLANNERS
    PLANNERS[queue] = strategy
