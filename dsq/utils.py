import sys
import signal
from uuid import uuid4
from base64 import urlsafe_b64encode
from itertools import islice

from redis import StrictRedis


def make_id():  # pragma: no cover
    """Make uniq short id"""
    return urlsafe_b64encode(uuid4().bytes).rstrip(b'=')


def task_fmt(task):
    arglist = []
    arglist.extend('{}'.format(r) for r in task.get('args', ()))
    arglist.extend('{}={}'.format(*r) for r in task.get('kwargs', {}).items())
    return '{}({})#{}'.format(task.get('name', '__no_name__'),
                              ', '.join(arglist), task.get('id', '__no_id__'))


def iter_chunks(seq, chunk_size):  # pragma: no cover
    it = iter(seq)
    while True:
        chunk = list(islice(it, chunk_size))
        if chunk:
            yield chunk
        else:
            break


class RunFlag(object):  # pragma: no cover
    def __init__(self):
        self._flag = True
        signal.signal(signal.SIGINT, self.handler)
        signal.signal(signal.SIGTERM, self.handler)

    def __nonzero__(self):
        return self._flag

    def stop(self):
        self._flag = False

    def handler(self, signal, frame):
        self.stop()


def redis_client(url):  # pragma: no cover
    if url:
        if not url.startswith('redis://'):
            url = 'redis://' + url
        return StrictRedis.from_url(url)
    else:
        return StrictRedis()


class LoadError(Exception):
    def __init__(self, var, module):
        self.var = var
        self.module = module


def load_var(module_name, default_var):
    """Loads variable from a module

    :param module_name: module.name or module.name:var
    :param default_var: default var name
    :raises ImportError: if module can't be imported
    :raises LoadError: if module has no var
    """
    module_name, _, mvar = module_name.partition(':')
    if not mvar:
        mvar = default_var

    __import__(module_name)
    module = sys.modules[module_name]
    manager = getattr(module, mvar, None)
    if not manager:
        raise LoadError(mvar, module_name)

    return manager


def load_manager(module_name):  # pragma: no cover
    try:
        return load_var(module_name, 'manager')
    except LoadError as e:
        print('{} not found in {}'.format(e.var, e.module))
        sys.exit(1)
