from __future__ import print_function

from time import time
import logging

from .utils import attrdict, make_id
from .worker import StopWorker
from .compat import PY2

log = logging.getLogger(__name__)


def make_task(name, args=None, kwargs=None, meta=None, expire=None,
              dead=None, retry=None, retry_delay=None, timeout=None, keep_result=None):
    return attrdict(id=make_id(), name=name, args=args or (), kwargs=kwargs or {},
                    meta=meta or {}, expire=expire, dead=dead, retry=retry,
                    retry_delay=retry_delay, timeout=timeout, keep_result=keep_result)


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
    """DSQ manager

    Allows to register task functions, push tasks and get task results

    :param queue: :py:class:`~.store.QueueStore` to use for tasks.
    :param result: :py:class:`~.store.ResultStore` to use for task results.
    :param sync: Synchronous operation. Task will be executed immediately during
                 :py:meth:`push` call.
    :param unknown: Name of unknown queue for tasks for which there is no registered functions.
                    Default is 'unknown'.
    :param default_queue: Name of default queue. Default is 'dsq'.
    """
    def __init__(self, queue, result=None, sync=False, unknown=None, default_queue=None):
        self.queue = queue
        self.result = result
        self.sync = sync
        self.registry = {}
        self.unknown = unknown or 'unknown'
        self.default_queue = default_queue or 'dsq'

    def task(self, name=None, queue=None, with_context=False, **kwargs):
        """Task decorator

        Function wrapper to register task in manager and provide simple interface to calling it.

        :param name: Task name, dsq will use func.__name__ if not provided.
        :param queue: Queue name to use.
        :param with_context: Provide task context as first task argument.
        :param \*\*kwrags: Rest params as for :py:meth:`push`.

        ::

            @manager.task
            def task1(arg):
                long_running_func(arg)

            @manager.task(name='custom-name', queue='low', with_context=True)
            def task2(ctx, arg):
                print ctx.task.id
                return long_running_func(arg)

            task1('boo')  # push task to queue
            task2.modify(keep_result=300)('foo')  # push task with keep_result option.
            task1.sync('boo')  # direct call of task1.
        """
        def decorator(func):
            fname = tname or func.__name__
            self.register(fname, func, with_context)
            return Task(self, func, queue=queue or self.default_queue, name=fname, **kwargs)

        if callable(name):
            tname = None
            return decorator(name)

        tname = name
        return decorator

    def register(self, name, func, with_context=False):
        """Register task

        :param name: Task name.
        :param func: Function.
        :param with_context: Provide task context as first task argument.

        ::

            def add(a, b):
                return a + b

            manager.register('add', add)
            manager.push('normal', 'add', (1, 2), keep_result=300)
        """
        if with_context:
            func._dsq_context = True
        self.registry[name] = func

    def push(self, queue, name, args=(), kwargs={}, meta=None, ttl=None,
             eta=None, delay=None, dead=None, retry=None, retry_delay=10,
             timeout=None, keep_result=None):
        """Add task into queue

        :param queue: Queue name.
        :param name: Task name.
        :param args: Task args.
        :param kwargs: Task kwargs.
        :param meta: Task additional info.
        :param ttl: Task time to live.
        :param eta: Schedule task execution for particular unix timestamp.
        :param delay: Postpone task execution for particular amount of seconds.
        :param dead: Name of dead-letter queue.
        :param retry: Retry task execution after exception. True - forever,
                      number - retry this amount.
        :param retry_delay: Delay between retry attempts.
        :param timeout: Task execution timeout.
        :param keep_result: Keep task return value for this amount of seconds.
                            Result is ignored by default.
        """
        if self.sync:
            self.process(make_task(name, args, kwargs, meta=meta))
            return

        if delay:
            eta = time() + delay

        task = make_task(name, args, kwargs, meta=meta, expire=ttl and (time() + ttl),
                         dead=dead, retry=retry, retry_delay=retry_delay,
                         timeout=timeout, keep_result=keep_result)
        self.queue.push(queue, task, eta=eta)
        return task.id if PY2 else task.id.decode()

    def pop(self, queue_list, timeout=None):
        """Pop item from the first not empty queue in ``queue_list``

        :param queue_list: List of queue names.
        :param timeout: Wait item for this amount of seconds (integer).
                        By default blocks forever.

        ::

            item = manager.pop(['high', 'normal'], 1)
            if item:
                manager.process(item)
        """
        queue, task = self.queue.pop(queue_list, timeout)
        if task:
            task['queue'] = queue
            return attrdict(task)

    def process(self, task, now=None, log_exc=True):
        """Process task item

        :param task: Task.
        :param now: Unix timestamp to compare with ``task.expire`` time and set ``eta`` on retry.
        :param log_exc: Log any exception during task execution. ``True`` by default.
        """
        if task.expire is not None and (now or time()) > task.expire:
            return

        try:
            func = self.registry[task.name]
        except KeyError:
            if self.sync:
                raise
            self.queue.push(self.unknown, task)
            log.error('Function for task "%s" not found', task.name)
            return

        try:
            if getattr(func, '_dsq_context', None):
                result = func(attrdict(manager=self, task=task), *task.args, **task.kwargs)
            else:
                result = func(*task.args, **task.kwargs)

            if task.get('keep_result'):
                self.result.set(task.id, result, task.keep_result)

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
                self.queue.push(task.queue, task, eta=eta)
            elif task.dead:
                task.retry = False
                self.queue.push(task.dead, task)
