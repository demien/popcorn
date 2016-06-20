class Machine(object):

    RECORD_NUMBER = 100

    _CPU_WINDOW_SIZE = 5
    _CPU_THS = 10  # 10 percent
    _MEMORY_THS = 500 * 1024 ** 2  # if remain memeory < this number , not start more worker

    def __init__(self, id):
        self.id = id
        self._original_stats = []
        self._plan = defaultdict(int)

    def update_stats(self, stats):
        print '[Machine] %s cpu:%s , memory:%s MB' % (self.id, self.cpu, self.memory / 1024 ** 2)
        self._original_stats.append(stats)
        if len(self._original_stats) > self.RECORD_NUMBER:
            self._original_stats.pop(0)

    @property
    def memory(self):
        if self._original_stats:
            return self._original_stats[-1]['memory'].available
        else:
            return self._MEMORY_THS + 100

    @property
    def cpu(self):
        if len(self._original_stats) >= self._CPU_WINDOW_SIZE:
            # return sum([i['cpu'].idle for i in self._original_stats[-self._CPU_WINDOW_SIZE:]]) / float(
            #     self._CPU_WINDOW_SIZE)
            if sum([1 for i in self._original_stats[-self._CPU_WINDOW_SIZE:] if i['cpu'].idle < 5]) > 1:
                return 100
            else:
                return 0
        else:
            return 50

    @property
    def health(self):
        return self.cpu >= self._CPU_THS and self.memory >= self._MEMORY_THS

    def get_worker_number(self, queue):
        return self._plan[queue]

    def current_worker_number(self, queue):
        return 1

    def plan(self, *queues):
        return {queue: self.get_worker_number(queue) for queue in queues}
        # import random
        # return {'pop': random.randint(1, 6)}

    # def update_plan(self, queue, worker_number):
    #     if self.health:
    #         support = self.memory / 100 * 1024 ** 2
    #         if worker_number <= support:
    #             self._plan[queue] = worker_number
    #         else:
    #             self._plan[queue] = support
    #     print '[Machine] %s take %d workers' % (self.id, self._plan.get(queue, 0))
    #     return self._plan[queue]  # WARNING should always return workers you take in

    def add_plan(self, queue, worker_number):
        self._plan[queue] += worker_number

    def clear_plan(self):
        self._plan = defaultdict(int)
