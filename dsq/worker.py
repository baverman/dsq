import sys
import logging
import signal
from time import time

log = logging.getLogger(__name__)
current_task = None


def alarm_handler(signum, frame):
    log.error('Timeout during processing task {id} {name}({args}, {kwargs})'.format(**current_task))
    sys.exit(1)


class Worker(object):
    def __init__(self, manager, queue_list, lifetime=None, task_timeout=None):
        self.manager = manager
        self.queue_list = queue_list
        self.lifetime = lifetime
        self.task_timeout = task_timeout

    def process_one(self, task):
        global current_task
        if self.task_timeout:
            signal.alarm(self.task_timeout)

        current_task = task
        self.manager.process(task)

        if self.task_timeout:
            signal.alarm(0)

    def process(manager, queue_list, lifetime, timeout):
        signal.signal(signal.SIGALRM, alarm_handler)
