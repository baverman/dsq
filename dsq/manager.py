from __future__ import print_function

import sys
from time import time
import logging

from .utils import attrdict, make_id
from .worker import StopWorker

log = logging.getLogger(__name__)


def make_task(name, args=None, kwargs=None, meta=None, expire=None):
    return attrdict(id=make_id(),
                    name=name,
                    args=args or (),
                    kwargs=kwargs or {},
                    meta=meta or {},
                    expire=expire)


def load_manager(module_name):
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


class Manager(object):
    def __init__(self, store, sync=False, unknown=None):
        self.store = store
        self.sync = sync
        self.registry = {}
        self.unknown = unknown or 'unknown'

    def async(self, name=None, with_context=False):
        def decorator(func):
            fname = name or func.__name__
            self.register(fname, func, with_context)
            return func
        return decorator

    def register(self, name, func, with_context=False):
        if with_context:
            func._dsq_context = True
        self.registry[name] = func

    def apply(self, queue, name, args=(), kwargs={}, meta=None,
              ttl=None, eta=None, delay=None):
        if self.sync:
            self.registry[name](*args, **kwargs)
            return

        if delay:
            eta = time() + delay

        task = make_task(name, args, kwargs, meta=meta, expire=ttl and (time() + ttl))
        return self.store.push(queue, task, eta=eta)

    def pop(self, queue_list, timeout=None):
        task = self.store.pop(queue_list, timeout)
        if task:
            return attrdict(task)

    def process(self, task, now=None):
        if task.expire is not None and (now or time()) > task.expire:
            return

        try:
            func = self.registry[task.name]
        except KeyError:
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
            log.exception('Error during processing task {id} {name}({args}, {kwargs})'.format(**task))
