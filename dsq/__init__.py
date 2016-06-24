_is_main = False


def create_store(url=None):
    from redis import StrictRedis
    from .store import Store

    if url:
        if not url.startswith('redis://'):
            url = 'redis://' + url
        cl = StrictRedis.from_url(url)
    else:
        cl = StrictRedis()

    return Store(cl)


def create_manager(url=None, sync=False, unknown=None):
    from .manager import Manager
    return Manager(create_store(url), sync, unknown)


def is_main():
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
