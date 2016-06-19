from uuid import uuid4
from base64 import urlsafe_b64encode
from itertools import islice


def make_id():
    """Make uniq short id"""
    return urlsafe_b64encode(uuid4().bytes).rstrip('=')


class attrdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


def iter_chunks(seq, chunk_size):
    it = iter(seq)
    while True:
        chunk = list(islice(it, chunk_size))
        if chunk:
            yield chunk
        else:
            break
