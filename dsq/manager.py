from time import time
import logging

from .utils import attrdict, make_id

log = logging.getLogger('dsq.manager')


def make_task(name, args=None, kwargs=None, meta=None, expire=None):
    return attrdict(id=make_id(),
                    name=name,
                    args=args or (),
                    kwargs=kwargs or {},
                    meta=meta or {},
                    expire=expire)


class Manager(object):
    def __init__(self, store, sync=False, unknown='unknown'):
        self.store = store
        self.sync = sync
        self.registry = {}
        self.unknown = unknown

    def async(self, name, with_context=False):
        def decorator(func):
            self.register(name, func, with_context)
            return func
        return decorator

    def register(self, name, func, with_context=False):
        if with_context:
            func._dsq_context = True
        self.registry[name] = func

    def apply(self, queue, name, args=(), kwargs={}, meta=None, ttl=None, eta=None):
        if self.sync:
            self.registry[name](*args, **kwargs)
            return

        task = make_task(name, args, kwargs, meta=meta, expire=ttl and (time() + ttl))
        return self.store.push(queue, task, eta=eta)

    def pop(self, queue_list, timeout=None):
        return attrdict(self.store.pop(queue_list, timeout))

    def process(self, task, now=None):
        if task.expire is not None and (now or time()) > task.expire:
            return

        try:
            func = self.registry[task.name]
        except KeyError:
            self.store.push(self.unknown, task)
            log.error('Function for task "%s" not found', task.name)
            return

        if getattr(func, '_dsq_context', None):
            func(attrdict(manager=self, task=task), *task.args, **task.kwargs)
        else:
            func(*task.args, **task.kwargs)
