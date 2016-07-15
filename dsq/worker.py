import traceback
import logging
import signal
import random
from time import time

from .utils import RunFlag, task_fmt

log = logging.getLogger(__name__)


class StopWorker(Exception):
    pass


class Worker(object):
    def __init__(self, manager, lifetime=None, task_timeout=None):
        self.manager = manager
        self.lifetime = lifetime and random.randint(lifetime, lifetime + lifetime // 10)
        self.task_timeout = task_timeout
        self.current_task = None

    def process_one(self, task):
        timeout = task.get('timeout', self.task_timeout)
        if timeout: signal.alarm(timeout)

        self.current_task = task
        log.info('Executing %s', task_fmt(task))
        self.manager.process(task)

        if timeout: signal.alarm(0)

    def alarm_handler(self, signum, frame):  # pragma: no cover
        trace = ''.join(traceback.format_stack(frame))
        log.error(
            'Timeout during processing task {}\n %s'.format(
                task_fmt(self.current_task)),
            trace)
        raise StopWorker()

    def process(self, queue_list, burst=False):  # pragma: no cover
        signal.signal(signal.SIGALRM, self.alarm_handler)

        run = RunFlag()
        start = time()
        while run:
            task = self.manager.pop(queue_list, 1)
            if task:
                try:
                    self.process_one(task)
                except StopWorker:
                    break
            elif burst:
                break

            if self.lifetime and time() - start > self.lifetime:
                break
