from popcorn.rpc.pyro import PyroServer
import json
import math
import traceback
import threading
import time
import os
from celery import bootsteps

import state
from popcorn.apps.base import BaseApp
from popcorn.apps.hub.order.instruction import Instruction, WorkerInstruction
from popcorn.rpc.pyro import RPCServer as _RPCServer
from popcorn.utils import get_log_obj, get_pid
from state import (
    DEMAND, PLAN, MACHINES, add_demand, remove_demand, add_plan, pop_order, update_machine, get_worker_cnt)

debug, info, warn, error, critical = get_log_obj(__name__)


class Hub(BaseApp):

    def __init__(self, app, **kwargs):
        self.app = app or self.app
        super(Hub, self).init(**kwargs)
        self.rpc_server = PyroServer()  # fix me, load it dynamiclly
        self.__shutdown_hub = threading.Event()
        self.__shutdown_demand_analyse = threading.Event()
        self.DEMAND_ANALYSE_INTERVAL = 10  # second

    def demand_analyse_loop(self, condition=lambda: True):
        debug('[Hub] - [Start] - [Demand analyse loop] : PID %s' % get_pid())
        while not self.__shutdown_demand_analyse.isSet() and condition:
            Hub.analyze_demand()
            time.sleep(self.DEMAND_ANALYSE_INTERVAL)
        debug('[Hub] - [Exit] - [Demand analyse loop]')

    def start(self, condition=lambda: True):
        """
        Start the hub.
        Step 1. Start rpc server
        Step 2. Start default planners thread
        Step 3. Start demand analyse loop thread
        Step 4. loop
        """
        self.__shutdown_hub.clear()
        self.__shutdown_demand_analyse.clear()

        self.rpc_server.start()

        from popcorn.apps.planner import schedule_planner
        for queue, strategy in self.app.conf.get('DEFAULT_QUEUE', {}).iteritems():
            schedule_planner(self.app, queue, strategy)

        thread = threading.Thread(target=self.demand_analyse_loop)
        thread.daemon = True
        thread.start()

        while not self.__shutdown_hub.isSet() and condition:
            continue

    @staticmethod
    def guard_heartbeat(machine):
        try:
            debug('[HUB] - [Receive] - Guard Heartbeat] : %s' % machine.id)
            update_machine(machine)
            return pop_order(machine.id)
        except Exception as e:
            traceback.print_exc();
            print e
            return None

    @staticmethod
    def analyze_demand():
        if not DEMAND:
            return
        debug("[Hub] - [Start] - [Analyze Demand] : %s. PID %s" % (json.dumps(DEMAND), get_pid()))
        success_queues = []
        for queue, worker_cnt in DEMAND.iteritems():
            if Hub.load_balancing(queue, worker_cnt):
                success_queues.append(queue)
        for queue in success_queues:
            remove_demand(queue)

    @staticmethod
    def report_demand(type, queue, result):
        debug('[Hub] - [Receive] - [Demand] - %s : %s' % (queue, result))
        instruction = Instruction.create(type, WorkerInstruction.generate_instruction_cmd(queue, result))
        current_worker_cnt = get_worker_cnt(instruction.queue)
        new_worker_cnt = instruction.operator.apply(current_worker_cnt, instruction.worker_cnt)
        add_demand(instruction.queue, new_worker_cnt)

    @staticmethod
    def guard_register(machine):
        info('[Guard] - [Receive] - [Machine Register] : %s' % machine.id)
        update_machine(machine)

    @staticmethod
    def load_balancing(queue, worker_cnt):
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
    def scan(target):
        from popcorn.apps.scan import ScanTarget
        from popcorn.apps.planner import Planner
        if target == ScanTarget.MACHINE:
            return dict(MACHINES)
        if target == ScanTarget.PLANNER:
            return Planner.stats()
