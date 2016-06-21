import logging
import signal
from time import time

log = logging.getLogger(__name__)
current_task = None


class StopWorker(Exception):
    pass


def alarm_handler(signum, frame):
    log.exception('Timeout during processing task {id} {name}({args}, {kwargs})'.format(**current_task))
    raise StopWorker()


class Worker(object):
    def __init__(self, manager, lifetime=None, task_timeout=None):
        self.manager = manager
        self.lifetime = lifetime
        self.task_timeout = task_timeout

    def process_one(self, task):
        timeout = task.timeout or self.task_timeout
        if timeout: signal.alarm(timeout)

        global current_task
        current_task = task
        self.manager.process(task)

        if timeout: signal.alarm(0)

    def process(self, queue_list):
        if self.task_timeout:
            signal.signal(signal.SIGALRM, alarm_handler)

        start = time()
        while True:
            task = self.manager.pop(queue_list, 1)
            if task:
                try:
                    self.process_one(task)
                except StopWorker:
                    break

            if self.lifetime and time() - start > self.lifetime:
                break
