import time
import pytest
import redis

from dsq.store import ResultStore


@pytest.fixture
def store(request):
    cl = redis.StrictRedis()
    cl.flushdb()
    return ResultStore(cl)


def test_set(store):
    store.set('id', 10, 20)
    assert store.get('id') == 10
    assert store.client.ttl('id') == 20


def test_empty_get(store):
    assert store.get('not-exists') == None
