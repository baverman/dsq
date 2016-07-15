import time
import pytest
import redis
import msgpack

from dsq.store import QueueStore


@pytest.fixture
def store(request):
    cl = redis.StrictRedis()
    cl.flushdb()
    return QueueStore(cl)


def test_push_pop(store):
    assert store.pop(['test'], 1) == (None, None)
    store.push('test', 't1')
    result = store.pop(['test'], 1)
    assert result == ('test', 't1')


def test_reschedule(store):
    store.push('test', 't1', eta=500)
    store.reschedule(now=490)
    assert store.pop(['test'], 1) == (None, None)
    store.reschedule(now=510)
    assert store.pop(['test'], 1)[1] == 't1'


def test_stat(store):
    store.push('boo', 't1', eta=500)
    store.push('boo', 't2')
    store.push('boo', 't3')
    assert store.stat() == {'schedule': 1, 'boo': 2}


def task_names(tasks):
    return [msgpack.loads(r) for r in tasks]


def stask_names(tasks):
    return [msgpack.loads(r[0].partition(b':')[2]) for r in tasks]


def test_take_and_put(store):
    store.push('boo', 'boo1')
    store.push('boo', 'boo2')
    store.push('boo', 'boo3')
    store.push('foo', 'foo1')
    store.push('foo', 'foo2')

    store.push('boo', 'boo4', eta=10)
    store.push('boo', 'boo5', eta=15)
    store.push('foo', 'foo3', eta=20)
    assert set(store.queue_list()) == set(('boo', 'foo'))

    result = store.take_many(2)
    assert stask_names(result['schedule']) == [b'boo4', b'boo5']
    assert task_names(result['queues']['boo']) == [b'boo1', b'boo2']
    assert task_names(result['queues']['foo']) == [b'foo1', b'foo2']

    result = store.take_many(10)
    assert stask_names(result['schedule']) == [b'foo3']
    assert task_names(result['queues']['boo']) == [b'boo3']
    assert 'foo' not in result['queues']

    store.put_many(result)
    assert set(store.queue_list()) == set(('boo',))

    assert store.get_schedule() == [(20, 'foo', 'foo3')]
    assert store.get_queue('boo') == ['boo3']
    assert store.get_queue('foo') == []
