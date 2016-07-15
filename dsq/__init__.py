version = '0.6'
_is_main = False


def create_manager(queue=None, result=None, sync=False,
                   unknown=None, default_queue=None):  # pragma: no cover
    from .manager import Manager
    from .store import QueueStore, ResultStore
    from .utils import redis_client
    '''Helper to create dsq manager

    :param queue: Redis url for queue store. [redis://]host[:port]/dbnum.
    :param result: Redis url for result store. By default it is the
                   same as queue. [redis://]host[:port]/dbnum.
    :returns: :py:class:`~.manager.Manager`

    ``sync``, ``unknown`` and ``default_queue`` params are the same as for
    :py:class:`~.manager.Manager` constructor.
    ::

       manager = create_manager()

       @manager.task(queue='high', keep_result=600)
       def add(a, b):
           return a + b
    '''
    return Manager(QueueStore(redis_client(queue)),
                   ResultStore(redis_client(result or queue)),
                   sync=sync, unknown=unknown, default_queue=default_queue)


def is_main():  # pragma: no cover
    '''Returns True if ``dsq`` command is in progress

    May be useful for tasks module which imports
    other tasks to avoid recursive imports::

        #tasks.py
        import dsq

        if dsq.is_main():
            import sms_tasks
            import billing_tasks
    '''
    return _is_main
