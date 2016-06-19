import time
import pytest
import redis
import msgpack

from dsq.store import Store


@pytest.fixture
def store(request):
    cl = redis.StrictRedis()
    cl.flushdb()
    return Store(cl)


def test_push_pop(store):
    assert not store.pop(['test'], 1)
    store.push('test', 't1')
    result = store.pop(['test'], 1)
    assert result == 't1'


def test_reschedule(store):
    store.push('test', 't1', eta=500)
    store.reschedule(now=490)
    assert not store.pop(['test'], 1)
    store.reschedule(now=510)
    assert store.pop(['test'], 1) == 't1'


def task_names(tasks):
    return [msgpack.loads(r) for r in tasks]


def stask_names(tasks):
    return [msgpack.loads(r[0].partition(':')[2]) for r in tasks]


def test_take_and_put(store):
    store.push('boo', 'boo1')
    store.push('boo', 'boo2')
    store.push('boo', 'boo3')
    store.push('foo', 'foo1')
    store.push('foo', 'foo2')

    store.push('boo', 'boo4', eta=10)
    store.push('boo', 'boo5', eta=15)
    store.push('foo', 'foo3', eta=20)

    result = store.take_many(2)
    assert stask_names(result['schedule']) == ['boo4', 'boo5']
    assert task_names(result['queues']['boo']) == ['boo1', 'boo2']
    assert task_names(result['queues']['foo']) == ['foo1', 'foo2']

    result = store.take_many(10)
    assert stask_names(result['schedule']) == ['foo3']
    assert task_names(result['queues']['boo']) == ['boo3']
    assert 'foo' not in result['queues']

    store.put_many(result)
    store.reschedule(now=50)
    assert store.pop(['boo', 'foo'], 1) == 'boo3'
    assert store.pop(['boo', 'foo'], 1) == 'foo3'
