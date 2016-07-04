from __future__ import print_function

import sys
from time import time
import logging

from .utils import attrdict, make_id
from .worker import StopWorker

log = logging.getLogger(__name__)


def make_task(name, args=None, kwargs=None, meta=None, expire=None,
              dead=None, retry=None, retry_delay=None, timeout=None):
    return attrdict(id=make_id(), name=name, args=args or (), kwargs=kwargs or {},
                    meta=meta or {}, expire=expire, dead=dead, retry=retry,
                    retry_delay=retry_delay, timeout=timeout)


def load_manager(module_name):  # pragma: no cover
    module_name, _, mvar = module_name.partition(':')
    if not mvar:
        mvar = 'app'

    __import__(module_name)
    module = sys.modules[module_name]
    manager = getattr(module, mvar, None)
    if not manager:
        print('{} not found in {}'.format(mvar, module_name))
        sys.exit(1)

    return manager


class Task(object):
    def __init__(self, manager, func, **kwargs):
        self.sync = func
        self.manager = manager
        self.ctx = kwargs

    def __call__(self, *args, **kwargs):
        self.manager.push(args=args, kwargs=kwargs, **self.ctx)

    def modify(self, **kwargs):
        ctx = self.ctx.copy()
        ctx.update(kwargs)
        return Task(self.manager, self.sync, **ctx)


class Manager(object):
    def __init__(self, store, sync=False, unknown=None):
        self.store = store
        self.sync = sync
        self.registry = {}
        self.unknown = unknown or 'unknown'

    def task(self, name=None, queue='dsq', with_context=False, **kwargs):
        def decorator(func):
            fname = tname or func.__name__
            self.register(fname, func, with_context)
            return Task(self, func, queue=queue, name=fname, **kwargs)

        if callable(name):
            tname = None
            return decorator(name)

        tname = name
        return decorator

    def register(self, name, func, with_context=False):
        if with_context:
            func._dsq_context = True
        self.registry[name] = func

    def push(self, queue, name, args=(), kwargs={}, meta=None, ttl=None,
             eta=None, delay=None, dead=None, retry=None, retry_delay=10, timeout=None):
        """Add tasks into queue

        :param queue: Queue name.
        :param name: Task name.
        :param args: Task args.
        :param kwargs: Task kwargs.
        :param meta: Task additional info.
        :param ttl: Task time to live.
        :param eta: Schedule task execution for particular unix timestamp.
        :param delay: Postpone task execution for particular amount of seconds.
        :param dead: Name of dead-letter queue.
        :param retry: Retry task execution after exception. True - forever, number - retry this amount.
        :param retry_delay: Delay between retry attempts.
        :param timeout: Task execution timeout.
        """
        if self.sync:
            self.process(make_task(name, args, kwargs, meta=meta))
            return

        if delay:
            eta = time() + delay

        task = make_task(name, args, kwargs, meta=meta, expire=ttl and (time() + ttl),
                         dead=dead, retry=retry, retry_delay=retry_delay, timeout=timeout)
        return self.store.push(queue, task, eta=eta)

    def pop(self, queue_list, timeout=None):
        queue, task = self.store.pop(queue_list, timeout)
        if task:
            task['queue'] = queue
            return attrdict(task)

    def process(self, task, now=None, log_exc=True):
        if task.expire is not None and (now or time()) > task.expire:
            return

        try:
            func = self.registry[task.name]
        except KeyError:
            if self.sync:
                raise
            self.store.push(self.unknown, task)
            log.error('Function for task "%s" not found', task.name)
            return

        try:
            if getattr(func, '_dsq_context', None):
                func(attrdict(manager=self, task=task), *task.args, **task.kwargs)
            else:
                func(*task.args, **task.kwargs)
        except StopWorker:
            raise
        except:
            if self.sync:
                raise

            if log_exc:
                log.exception('Error during processing task {id} {name}({args}, {kwargs})'.format(**task))

            if task.retry and task.retry > 0:
                if task.retry is not True:
                    task.retry -= 1
                eta = task.retry_delay and (now or time()) + task.retry_delay
                self.store.push(task.queue, task, eta=eta)
            elif task.dead:
                task.retry = False
                self.store.push(task.dead, task)
