import signal
from uuid import uuid4
from base64 import urlsafe_b64encode
from itertools import islice

from redis import StrictRedis


def make_id():  # pragma: no cover
    """Make uniq short id"""
    return urlsafe_b64encode(uuid4().bytes).rstrip(b'=')


class attrdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


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
