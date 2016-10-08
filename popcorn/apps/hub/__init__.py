import json
import math
import traceback
import threading
import time
import os
from collections import defaultdict
from celery import bootsteps
from popcorn.apps.base import BaseApp
from popcorn.apps.hub.order import Order
from popcorn.apps.hub.order.instruction import Instruction, WorkerInstruction, InstructionType
from popcorn.rpc.pyro import PyroServer, PyroClient
from popcorn.utils import get_log_obj, get_pid
from .state import DEMAND, PLAN, MACHINES, add_demand, remove_demand, add_plan, pop_order, get_worker_cnt


debug, info, warn, error, critical = get_log_obj(__name__)


class Hub(BaseApp):

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        super(Hub, self).init(**kwargs)

        self.rpc_server = PyroServer()  # fix me, load it dynamiclly
        self.__shutdown_hub = threading.Event()
        self.LOOP_INTERVAL = 10  # second
        self.guard_client = defaultdict(lambda: None)

    def start(self, condition=lambda: True):
        """
        Start the hub.
        Step 1. Start rpc server
        Step 2. Start default planners thread
        Step 3. Start loop
        """
        self.__shutdown_hub.clear()

        self._start_rpc_server()
        self._start_default_planners()
        self._start_loop(condition)

    def get_guard_client(self, ip):
        if self.guard_client[ip] is None:
            self.guard_client[ip] = PyroClient(ip)
        return self.guard_client[ip]

    def _start_rpc_server(self):
        self.rpc_server.start()

    def _start_default_planners(self):
        from popcorn.apps.planner.commands import start_planner
        for queue, strategy in self.app.conf.get('DEFAULT_QUEUE', {}).iteritems():
            start_planner(self.app, queue, strategy)

    def _start_loop(self, condition):
        """
        Things to do:
        1. Anaylize demand
        2. Send order to guard (to do)
        3. Check healthy for guard & planner (to do)
        """
        debug('[Hub] - [Start] - [Loop] : PID %s' % get_pid())
        while not self.__shutdown_hub.isSet() and condition:
            self.analyze_demand()
            self.send_order_to_guard()
            time.sleep(self.LOOP_INTERVAL)
        debug('[Hub] - [Exit] - [Loop]')

    def send_order_to_guard(self):
        machine_order = defaultdict(lambda: Order())
        # construct order
        for queue, plan in PLAN.iteritems():
            for machine_id in plan.keys():
                worker_cnt = PLAN[queue].pop(machine_id)
                instruction_cmd = WorkerInstruction.dump(queue, worker_cnt)
                machine_order[machine_id].add_instruction(instruction_cmd)
        # send order
        for machine_id, order in machine_order.iteritems():
            self.get_guard_client(machine_id).call('popcorn.apps.guard.commands.receive_order', order)


    def analyze_demand(self):
        if not DEMAND:
            return
        debug("[Hub] - [Start] - [Analyze Demand] : %s. PID %s" % (json.dumps(DEMAND), get_pid()))
        success_queues = []
        for queue, worker_cnt in DEMAND.iteritems():
            if self.load_balancing(queue, worker_cnt):
                success_queues.append(queue)
        for queue in success_queues:
            remove_demand(queue)

    def load_balancing(self, queue, worker_cnt):
        healthy_machines = [machine for machine in MACHINES.values() if machine.snapshots[-1]['healthy']]
        if not healthy_machines:
            warn('[Hub] - [Warning] : No Healthy Machines!')
            return False
        worker_per_machine = int(math.ceil(worker_cnt / len(healthy_machines)))
        for machine in healthy_machines:
            info('[Hub] - [Start] - [Load Balance] : {%s} take %d workers on #%s' % (machine.id, worker_per_machine, queue))
            add_plan(queue, machine.id, worker_per_machine)
        return True

    @staticmethod
    def report_demand(type, queue, result):
        debug('[Hub] - [Receive] - [Demand] - %s : %s' % (queue, result))
        instruction = Instruction.create(type, WorkerInstruction.dump(queue, result))
        current_worker_cnt = get_worker_cnt(instruction.queue)
        new_worker_cnt = instruction.operator.apply(current_worker_cnt, instruction.worker_cnt)
        add_demand(instruction.queue, new_worker_cnt)
