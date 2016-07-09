from .manager import Manager
from .store import QueueStore, ResultStore
from .utils import redis_client

_is_main = False


def create_manager(queue=None, result=None, sync=False, unknown=None):  # pragma: no cover
    '''Creates dsq manager

    :param queue: Redis url for queue store. [redis://]host[:port]/dbnum.
    :param result: Redis url for result store. By default it is the
                   same as queue. [redis://]host[:port]/dbnum.
    :param sync: Synchronous operation. Manager.push invokes
                 task immediately.
    :param unknown: Name of unknown queue.

    ::

       manager = create_manager()

       @manager.task(queue='high', keep_result=600)
       def add(a, b):
           return a + b
    '''
    return Manager(QueueStore(redis_client(queue)),
                   ResultStore(redis_client(result or queue)),
                   sync,
                   unknown)


def is_main():  # pragma: no cover
    '''Returns True if dsq command is in progress

    May be useful for tasks module which imports
    other tasks to avoid recursive imports::

        #tasks.py
        import dsq

        if dsq.is_main():
            import sms_tasks
            import billing_tasks
    '''
    return _is_main
